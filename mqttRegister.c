//
// FILE            mqttRegister.c
//
// AUTHOR          Ken Zangelin
//
// Copyright 2026 Seamware
// SPDX-License-Identifier: Apache-2.0
//
// The MQTT bridge - libmosquitto, out of the broker.
//
// NOTIFICATIONS: a Subscription whose notification.endpoint.uri is mqtt:// or mqtts://
// (TS 104 243) is published through notify(), bridge ABI 10. Everything about the delivery is in the
// Subscription, as NGSI-LD defines it:
//
//   uri           mqtt[s]://[<user>[:<pass>]@]<host>[:<port>]/<topic>[/<subtopic>]*
//   notifierInfo  MQTT-QoS (0, 1, 2 - default 0), MQTT-Version (mqtt3.1.1, mqtt5.0 - default mqtt5.0)
//
// The message itself - the { "metadata": ..., "body": ... } envelope - is the broker's; this plugin
// sends it as it is given. One connection per notification, as before the move: connect, publish,
// wait until it has left (QoS 0) or been acknowledged (QoS 1/2), disconnect.
//
// CHANNELS: one MQTT topic <-> one entity attribute, as for every bridge. The Bridge is ONE
// connection, to the server its configuration names - in the same vocabulary a Subscription uses:
//
//   "mqtt": { "server": { "uri": "mqtt[s]://[user[:pass]@]host[:port]", "MQTT-Version": "mqtt5.0",
//                         "clientId": "coraine" },
//             "ngsild": { "topics": { "plant/barn001/filling": { "entityId": ..., "entityType": ...,
//                                                                 "attribute": "filling",
//                                                                 "channelInfo": [ { "key": "MQTT-QoS", "value": "1" } ] } } } }
//
// The endpoint is the topic, exact - no + or # wildcard: an arriving message finds its Channel by
// its topic. A message's payload is the value: JSON as it is, anything else as a JSON string. A
// write to the attribute is published to the topic. The plugin's own publications do not come back
// as samples: MQTT 5's no-local subscription option, and for 3.1.1 the last payload published on
// the topic, dropped once when it echoes.
//
// libmosquitto's threaded loop owns the connection - reconnects included, every inbound topic
// subscribed again on each (re)connect.
//
#include <stdbool.h>                                  // bool
#include <stdio.h>                                    // snprintf
#include <stdlib.h>                                   // strtol, free
#include <string.h>                                   // strncmp, strchr, strrchr, strlen, strdup
#include <strings.h>                                  // strcasecmp
#include <pthread.h>                                  // pthread_once, pthread_mutex_*
#include <time.h>                                     // clock_gettime

#include <mosquitto.h>                                // mosquitto_*
#include <mqtt_protocol.h>                            // MQTT_SUB_OPT_NO_LOCAL

#include "corLog/corLog.h"                            // COR_E, COR_W
#include "corAlloc/CorAlloc.h"                        // CorAlloc
#include "corAlloc/corAllocBufferInit.h"              // corAllocBufferInit
#include "corAlloc/corAllocBufferReset.h"             // corAllocBufferReset
#include "corJson/CorJson.h"                          // CorJson
#include "corJson/corJsonCreate.h"                    // corJsonCreate
#include "corJson/corJsonParse.h"                     // corJsonParse
#include "corTree/CorNode.h"                          // CorNode
#include "corTree/corTreeLookup.h"                    // corTreeLookup
#include "corBase/corFileReadInto.h"                  // corFileReadInto

#include "corBridge/BridgeDriver.h"                   // BridgeDriver, BridgeRegisterFunc
#include "corBridge/BridgeBroker.h"                   // BridgeBroker, BRIDGE_*



// -----------------------------------------------------------------------------
//
// The broker's side of the seam - kept from init() until close()
//
static const BridgeBroker* brokerP = NULL;



// -----------------------------------------------------------------------------
//
// MqttUri - an mqtt[s]:// URI taken apart; every string points into one strdup'd buffer
//
typedef struct MqttUri
{
  char*  buf;          // the copy the fields point into - free() it
  bool   tls;
  char*  user;         // NULL if absent
  char*  pass;         // NULL if absent
  char*  host;
  int    port;
  char*  topic;        // with any /-separated subtopics, no leading '/'
} MqttUri;



// -----------------------------------------------------------------------------
//
// uriParse - mqtt[s]://[user[:pass]@]host[:port]/topic
//
static bool uriParse(const char* uri, MqttUri* uP)
{
  memset(uP, 0, sizeof(*uP));

  const char* rest;

  if (strncmp(uri, "mqtt://", 7) == 0)
  {
    uP->port = 1883;
    rest     = &uri[7];
  }
  else if (strncmp(uri, "mqtts://", 8) == 0)
  {
    uP->tls  = true;
    uP->port = 8883;
    rest     = &uri[8];
  }
  else
    return false;

  if ((uP->buf = strdup(rest)) == NULL)
    return false;

  char* slash = strchr(uP->buf, '/');
  if ((slash == NULL) || (slash[1] == 0))             // no topic
    return false;

  *slash    = 0;
  uP->topic = &slash[1];

  char* authority = uP->buf;
  char* at        = strchr(authority, '@');

  if (at != NULL)
  {
    *at      = 0;
    uP->user = authority;

    char* colon = strchr(authority, ':');
    if (colon != NULL)
    {
      *colon   = 0;
      uP->pass = &colon[1];
    }

    authority = &at[1];
  }

  char* portColon = strrchr(authority, ':');
  if ((portColon != NULL) && (portColon[1] != 0))
  {
    *portColon = 0;

    long port = strtol(&portColon[1], NULL, 10);
    if ((port > 0) && (port < 65536))
      uP->port = (int) port;
  }

  uP->host = authority;

  return (uP->host[0] != 0);
}



// -----------------------------------------------------------------------------
//
// infoSettings - MQTT-QoS and MQTT-Version out of the notifierInfo JSON text
//
// The broker validated both when the Subscription was created; a key this plugin does not know is
// not its business (receiverInfo-like keys belong to the envelope, which the broker built).
//
static void infoSettings(const char* info, int* qosP, int* versionP)
{
  *qosP     = 0;
  *versionP = MQTT_PROTOCOL_V5;                       // § 7.2 Table 7.2-1: the default is mqtt5.0

  if (info == NULL)
    return;

  CorAlloc ka;
  CorJson  cj;
  char     buf[4096];
  char*    text = strdup(info);

  if (text == NULL)
    return;

  corAllocBufferInit(&ka, buf, sizeof(buf), 4096, NULL, "mqtt notifierInfo");
  corJsonCreate(&cj, &ka);

  CorNode* arrayP = corJsonParse(&cj, text);

  for (CorNode* pairP = ((arrayP != NULL) && (arrayP->type == CorArray)) ? arrayP->value.head : NULL; pairP != NULL; pairP = pairP->next)
  {
    CorNode* keyP = corTreeLookup(pairP, "key");
    CorNode* valP = corTreeLookup(pairP, "value");

    if ((keyP == NULL) || (keyP->type != CorString) || (valP == NULL) || (valP->type != CorString))
      continue;

    if (strcasecmp(keyP->value.s, "MQTT-QoS") == 0)
    {
      long qos = strtol(valP->value.s, NULL, 10);
      if ((qos >= 0) && (qos <= 2))
        *qosP = (int) qos;
    }
    else if (strcasecmp(keyP->value.s, "MQTT-Version") == 0)
    {
      if (strcasecmp(valP->value.s, "mqtt3.1.1") == 0)
        *versionP = MQTT_PROTOCOL_V311;
      else if (strcasecmp(valP->value.s, "mqtt5.0") == 0)
        *versionP = MQTT_PROTOCOL_V5;
    }
  }

  corAllocBufferReset(&ka, false);
  free(text);
}



// -----------------------------------------------------------------------------
//
// libInit - mosquitto_lib_init, once per process, whichever thread gets here first
//
// mosquitto_lib_init is not thread-safe, and two notifications on two threads can be the first at
// the same moment.
//
static int  libInitResult = -1;

static void libInitOnce(void)
{
  libInitResult = (mosquitto_lib_init() == MOSQ_ERR_SUCCESS) ? 0 : -1;
}

static int libInit(void)
{
  static pthread_once_t once = PTHREAD_ONCE_INIT;

  pthread_once(&once, libInitOnce);
  return libInitResult;
}



// -----------------------------------------------------------------------------
//
// mqttNotify - one notification: connect, publish, wait until it has gone, disconnect
//
static int mqttNotify(const char* url, const char* payload, const char* info, bool tlsInsecure)
{
  if (libInit() != 0)
    return BRIDGE_ERR;

  MqttUri u;

  if (uriParse(url, &u) == false)
  {
    COR_W("mqtt: notification endpoint '%s' is not mqtt[s]://[user[:pass]@]host[:port]/topic", url);
    free(u.buf);
    return BRIDGE_BAD_INPUT;
  }

  int qos;
  int version;
  infoSettings(info, &qos, &version);

  struct mosquitto* mosq = mosquitto_new(NULL, true, NULL);    // client id generated, clean session
  if (mosq == NULL)
  {
    free(u.buf);
    return BRIDGE_ERR;
  }

  bool ok = false;

  do
  {
    if (mosquitto_int_option(mosq, MOSQ_OPT_PROTOCOL_VERSION, version) != MOSQ_ERR_SUCCESS)
      break;

    if ((u.user != NULL) && (mosquitto_username_pw_set(mosq, u.user, u.pass) != MOSQ_ERR_SUCCESS))
      break;

    if (u.tls == true)
    {
      if (mosquitto_tls_set(mosq, NULL, "/etc/ssl/certs", NULL, NULL, NULL) != MOSQ_ERR_SUCCESS)
        break;

      if (tlsInsecure == true)                         // --insecureNotif: a self-signed endpoint is accepted
      {
        mosquitto_tls_opts_set(mosq, 0, NULL, NULL);   // SSL_VERIFY_NONE
        mosquitto_tls_insecure_set(mosq, true);
      }
    }

    if (mosquitto_connect(mosq, u.host, u.port, 30) != MOSQ_ERR_SUCCESS)
      break;

    if (mosquitto_publish(mosq, NULL, u.topic, strlen(payload), payload, qos, false) != MOSQ_ERR_SUCCESS)
      break;

    //
    // Pump the loop until the message has really gone. want_write going false means CONNECT and
    // PUBLISH have left the socket - over TLS the handshake alone spans several cycles, so one 100 ms
    // cycle and a disconnect drops the publish. QoS 0 then gets a few more cycles, or the teardown
    // races the receiving broker's routing (seen over TLS). QoS 1/2 wait for their acknowledgement.
    //
    int msLeft    = 5000;
    int postFlush = 0;

    while (msLeft > 0)
    {
      int rc = mosquitto_loop(mosq, 100, 1);
      if (rc != MOSQ_ERR_SUCCESS)
        break;

      msLeft -= 100;

      if (qos == 0)
      {
        if ((mosquitto_want_write(mosq) == false) && (++postFlush >= 3))
        {
          ok = true;
          break;
        }
      }
      else if (msLeft <= 4500)
      {
        ok = true;
        break;
      }
    }

    mosquitto_disconnect(mosq);
  } while (0);

  mosquitto_destroy(mosq);

  if (ok == false)
    COR_W("mqtt: notification failed: host=%s port=%d topic=%s", u.host, u.port, u.topic);

  free(u.buf);

  return (ok == true) ? BRIDGE_OK : BRIDGE_ERR;
}



// -----------------------------------------------------------------------------
//
// MqttChannel - one topic the Bridge carries
//
typedef struct MqttChannel
{
  char*                topic;
  BridgeDirection      direction;
  int                  qos;
  char*                lastPublished;   // 3.1.1 only: dropped once when it echoes back
  struct MqttChannel*  next;
} MqttChannel;



// -----------------------------------------------------------------------------
//
// The Bridge's connection, and its Channels (under mtx: the broker adds them, libmosquitto's thread reads them)
//
static struct mosquitto*  conn       = NULL;
static int                connVersion = MQTT_PROTOCOL_V5;
static char               serverDesc[256];   // host:port, for the log - never the credentials
static MqttChannel*       channels   = NULL;
static pthread_mutex_t    mtx        = PTHREAD_MUTEX_INITIALIZER;



// -----------------------------------------------------------------------------
//
// nowNs - the wall clock in nanoseconds: MQTT messages carry no time of their own
//
static int64_t nowNs(void)
{
  struct timespec ts;

  clock_gettime(CLOCK_REALTIME, &ts);
  return (int64_t) ts.tv_sec * 1000000000 + ts.tv_nsec;
}



// -----------------------------------------------------------------------------
//
// channelFind - under mtx
//
static MqttChannel* channelFind(const char* topic)
{
  for (MqttChannel* cP = channels; cP != NULL; cP = cP->next)
  {
    if (strcmp(cP->topic, topic) == 0)
      return cP;
  }

  return NULL;
}



// -----------------------------------------------------------------------------
//
// channelSubscribe - one inbound topic, on the live connection; under mtx
//
static void channelSubscribe(MqttChannel* cP)
{
  if ((conn == NULL) || (cP->direction == BridgeDirectionOut))
    return;

  int rc = (connVersion == MQTT_PROTOCOL_V5) ? mosquitto_subscribe_v5(conn, NULL, cP->topic, cP->qos, MQTT_SUB_OPT_NO_LOCAL, NULL)
                                             : mosquitto_subscribe(conn, NULL, cP->topic, cP->qos);

  if (rc != MOSQ_ERR_SUCCESS)
    COR_W("mqtt: cannot subscribe to '%s': %s", cP->topic, mosquitto_strerror(rc));
}



// -----------------------------------------------------------------------------
//
// onConnect - libmosquitto's thread: (re)connected - every inbound topic is subscribed again
//
static void onConnect(struct mosquitto* mosq, void* userdata, int rc)
{
  (void) mosq;
  (void) userdata;

  if (rc != 0)
  {
    COR_W("mqtt: %s refused the connection: %s", serverDesc, mosquitto_connack_string(rc));
    return;
  }

  COR_V("mqtt: connected to %s", serverDesc);

  pthread_mutex_lock(&mtx);
  for (MqttChannel* cP = channels; cP != NULL; cP = cP->next)
    channelSubscribe(cP);
  pthread_mutex_unlock(&mtx);
}



// -----------------------------------------------------------------------------
//
// onDisconnect - libmosquitto's thread: it reconnects by itself (mosquitto_reconnect_delay_set)
//
static void onDisconnect(struct mosquitto* mosq, void* userdata, int rc)
{
  (void) mosq;
  (void) userdata;

  if (rc != 0)
    COR_W("mqtt: lost the connection to %s - reconnecting", serverDesc);
}



// -----------------------------------------------------------------------------
//
// jsonOf - a payload as JSON text: as it is when it parses, else a JSON string; malloc'd
//
static char* jsonOf(const char* payload, int len)
{
  char* text = malloc(len + 1);
  if (text == NULL)
    return NULL;

  memcpy(text, payload, len);
  text[len] = 0;

  //
  // corJsonParse works in place, on a copy, and the answer is only "does it parse"
  //
  CorAlloc ka;
  CorJson  cj;
  char     buf[8192];
  char*    probe = strdup(text);
  bool     isJson;

  corAllocBufferInit(&ka, buf, sizeof(buf), 8192, NULL, "mqtt payload");
  corJsonCreate(&cj, &ka);
  isJson = (probe != NULL) && (len > 0) && (corJsonParse(&cj, probe) != NULL);
  corAllocBufferReset(&ka, false);
  free(probe);

  if (isJson == true)
    return text;

  //
  // Not JSON - a bare word, a CSV line: the value is the text, as a JSON string
  //
  char* out = malloc(len * 6 + 3);
  char* oP  = out;

  if (out == NULL)
  {
    free(text);
    return NULL;
  }

  *oP++ = '"';
  for (int i = 0; i < len; i++)
  {
    unsigned char c = (unsigned char) text[i];

    if      (c == '"')   { *oP++ = '\\'; *oP++ = '"';  }
    else if (c == '\\')  { *oP++ = '\\'; *oP++ = '\\'; }
    else if (c == '\n')  { *oP++ = '\\'; *oP++ = 'n';  }
    else if (c == '\r')  { *oP++ = '\\'; *oP++ = 'r';  }
    else if (c == '\t')  { *oP++ = '\\'; *oP++ = 't';  }
    else if (c < 0x20)   { oP += sprintf(oP, "\\u%04x", c); }
    else                 *oP++ = c;
  }
  *oP++ = '"';
  *oP   = 0;

  free(text);
  return out;
}



// -----------------------------------------------------------------------------
//
// onMessage - libmosquitto's thread: a message on a topic of ours becomes a sample
//
static void onMessage(struct mosquitto* mosq, void* userdata, const struct mosquitto_message* msgP)
{
  (void) mosq;
  (void) userdata;

  bool echo  = false;
  bool known = false;

  pthread_mutex_lock(&mtx);
  MqttChannel* cP = channelFind(msgP->topic);
  if ((cP != NULL) && (cP->direction != BridgeDirectionOut))
  {
    known = true;

    //
    // 3.1.1 has no no-local option: what this plugin published on the topic comes back, once
    //
    if ((cP->lastPublished != NULL) && ((int) strlen(cP->lastPublished) == msgP->payloadlen) &&
        (memcmp(cP->lastPublished, msgP->payload, msgP->payloadlen) == 0))
    {
      echo = true;
      free(cP->lastPublished);
      cP->lastPublished = NULL;
    }
  }
  pthread_mutex_unlock(&mtx);

  if ((known == false) || (echo == true))
    return;

  char* json = jsonOf((const char*) msgP->payload, msgP->payloadlen);
  if (json == NULL)
    return;

  //
  // Outside the lock: the broker takes its own inside sampleIn
  //
  brokerP->sampleIn("mqtt", msgP->topic, json, nowNs());
  free(json);
}



// -----------------------------------------------------------------------------
//
// serverConfig - the Bridge's connection, from the "mqtt" member of the --bridgeConfig file
//
// Returns false when there is no server - a Bridge for notifications only, which is fine - and
// sets *errorP when there is one that cannot be used.
//
static bool serverConfig(const char* configFile, MqttUri* uP, char* clientId, int clientIdSize, int* versionP, bool* errorP)
{
  static char text[64 * 1024];
  bool        found = false;

  *errorP   = false;
  *versionP = MQTT_PROTOCOL_V5;

  if ((configFile == NULL) || (corFileReadInto(configFile, text, sizeof(text)) < 0))
    return false;

  CorAlloc ka;
  CorJson  cj;
  char     buf[16 * 1024];

  corAllocBufferInit(&ka, buf, sizeof(buf), 16 * 1024, NULL, "mqtt config");
  corJsonCreate(&cj, &ka);

  CorNode* rootP   = corJsonParse(&cj, text);
  CorNode* bridgeP = (rootP   != NULL) ? corTreeLookup(rootP, "mqtt")     : NULL;
  CorNode* serverP = (bridgeP != NULL) ? corTreeLookup(bridgeP, "server") : NULL;

  if (serverP != NULL)
  {
    CorNode* uriP     = corTreeLookup(serverP, "uri");
    CorNode* versionNodeP = corTreeLookup(serverP, "MQTT-Version");
    CorNode* idP      = corTreeLookup(serverP, "clientId");

    found = true;

    if ((uriP == NULL) || (uriP->type != CorString))
    {
      COR_E("mqtt: server.uri is missing - mqtt[s]://[user[:pass]@]host[:port]");
      *errorP = true;
    }
    else
    {
      //
      // A Subscription's URI with the topic left out: the parser wants one, so a placeholder goes on.
      // A path given here would be a topic, and topics are the Channels' - refused, not ignored.
      //
      const char* authority = strstr(uriP->value.s, "://");
      char        withTopic[1024];

      if ((authority == NULL) || (strchr(&authority[3], '/') != NULL))
      {
        COR_E("mqtt: server.uri '%s' is not mqtt[s]://[user[:pass]@]host[:port] - no topic: those are the Channels'", uriP->value.s);
        *errorP = true;
      }
      else
      {
        snprintf(withTopic, sizeof(withTopic), "%s/_", uriP->value.s);
        if (uriParse(withTopic, uP) == false)
        {
          COR_E("mqtt: server.uri '%s' is not mqtt[s]://[user[:pass]@]host[:port]", uriP->value.s);
          *errorP = true;
        }
      }
    }

    if ((versionNodeP != NULL) && (versionNodeP->type == CorString))
    {
      if      (strcasecmp(versionNodeP->value.s, "mqtt3.1.1") == 0) *versionP = MQTT_PROTOCOL_V311;
      else if (strcasecmp(versionNodeP->value.s, "mqtt5.0")   == 0) *versionP = MQTT_PROTOCOL_V5;
      else
      {
        COR_E("mqtt: server.MQTT-Version must be 'mqtt3.1.1' or 'mqtt5.0'");
        *errorP = true;
      }
    }

    snprintf(clientId, clientIdSize, "%s", ((idP != NULL) && (idP->type == CorString)) ? idP->value.s : "");
  }

  corAllocBufferReset(&ka, false);
  return found;
}



// -----------------------------------------------------------------------------
//
// connectionOpen - the Bridge's connection, handed to libmosquitto's own thread
//
// connect_async: the broker's startup does not wait for, or fail on, an MQTT server that is not up
// yet - the loop keeps trying, and the Channels are subscribed when it answers.
//
static int connectionOpen(MqttUri* uP, const char* clientId, int version, bool tlsInsecure)
{
  conn = mosquitto_new((clientId[0] != 0) ? clientId : NULL, true, NULL);
  if (conn == NULL)
    return BRIDGE_ERR;

  connVersion = version;
  snprintf(serverDesc, sizeof(serverDesc), "%s:%d", uP->host, uP->port);

  mosquitto_int_option(conn, MOSQ_OPT_PROTOCOL_VERSION, version);
  if (uP->user != NULL)
    mosquitto_username_pw_set(conn, uP->user, uP->pass);

  if (uP->tls == true)
  {
    mosquitto_tls_set(conn, NULL, "/etc/ssl/certs", NULL, NULL, NULL);
    if (tlsInsecure == true)
    {
      mosquitto_tls_opts_set(conn, 0, NULL, NULL);
      mosquitto_tls_insecure_set(conn, true);
    }
  }

  mosquitto_connect_callback_set(conn, onConnect);
  mosquitto_disconnect_callback_set(conn, onDisconnect);
  mosquitto_message_callback_set(conn, onMessage);
  mosquitto_reconnect_delay_set(conn, 1, 30, true);

  int rc = mosquitto_connect_async(conn, uP->host, uP->port, 30);
  if ((rc != MOSQ_ERR_SUCCESS) && (rc != MOSQ_ERR_ERRNO))   // ERRNO: not up yet - the loop retries
  {
    COR_E("mqtt: cannot connect to %s: %s", serverDesc, mosquitto_strerror(rc));
    return BRIDGE_ERR;
  }

  if (mosquitto_loop_start(conn) != MOSQ_ERR_SUCCESS)
  {
    COR_E("mqtt: cannot start libmosquitto's thread");
    return BRIDGE_ERR;
  }

  COR_V("mqtt: Bridge connection to %s (MQTT %s)", serverDesc, (version == MQTT_PROTOCOL_V5) ? "5.0" : "3.1.1");
  return BRIDGE_OK;
}



// -----------------------------------------------------------------------------
//
// channelInfoQos - MQTT-QoS out of a Channel's channelInfo; false on a key or a value that does not fit
//
static bool channelInfoQos(const char* info, int* qosP, const char** whyP)
{
  *qosP = 0;

  if (info == NULL)
    return true;

  CorAlloc ka;
  CorJson  cj;
  char     buf[4096];
  char*    text = strdup(info);
  bool     ok;

  if (text == NULL)
    return false;

  corAllocBufferInit(&ka, buf, sizeof(buf), 4096, NULL, "mqtt channelInfo");
  corJsonCreate(&cj, &ka);

  CorNode* arrayP = corJsonParse(&cj, text);
  ok = (arrayP != NULL) && (arrayP->type == CorArray);

  for (CorNode* pairP = (ok == true) ? arrayP->value.head : NULL; (pairP != NULL) && (ok == true); pairP = pairP->next)
  {
    CorNode*    keyP = corTreeLookup(pairP, "key");
    CorNode*    valP = corTreeLookup(pairP, "value");
    const char* key  = ((keyP != NULL) && (keyP->type == CorString)) ? keyP->value.s : "";
    const char* val  = ((valP != NULL) && (valP->type == CorString)) ? valP->value.s : "";

    if (strcasecmp(key, "MQTT-QoS") == 0)
    {
      ok = ((strcmp(val, "0") == 0) || (strcmp(val, "1") == 0) || (strcmp(val, "2") == 0));
      if (ok == true)
        *qosP = val[0] - '0';
      else
        *whyP = "MQTT-QoS must be '0', '1' or '2'";
    }
    else
    {
      ok    = false;
      *whyP = "unknown key (MQTT-QoS is the one there is)";
    }
  }

  corAllocBufferReset(&ka, false);
  free(text);
  return ok;
}



// -----------------------------------------------------------------------------
//
// mqttChannelAddInfo - start carrying a topic
//
static int mqttChannelAddInfo(const char* endpoint, BridgeChannelKind kind, BridgeDirection direction, const char* info)
{
  const char* why = NULL;
  int         qos;

  if (kind != BridgeChannelTopic)
  {
    COR_E("mqtt: '%s': MQTT carries topics only", endpoint);
    return BRIDGE_UNSUPPORTED;
  }

  if (conn == NULL)
  {
    COR_E("mqtt: '%s': this Bridge has no server - set mqtt.server.uri in the bridge configuration", endpoint);
    return BRIDGE_BAD_INPUT;
  }

  if ((endpoint[0] == 0) || (strpbrk(endpoint, "+#") != NULL))
  {
    COR_E("mqtt: '%s' is not a topic a Channel can be - no + or # wildcards: a message finds its Channel by its topic", endpoint);
    return BRIDGE_BAD_INPUT;
  }

  if (channelInfoQos(info, &qos, &why) == false)
  {
    COR_E("mqtt: '%s': %s", endpoint, (why != NULL) ? why : "channelInfo is not an array of {key, value}");
    return BRIDGE_BAD_INPUT;
  }

  pthread_mutex_lock(&mtx);

  if (channelFind(endpoint) != NULL)
  {
    pthread_mutex_unlock(&mtx);
    return BRIDGE_OK;
  }

  MqttChannel* cP = calloc(1, sizeof(MqttChannel));
  if ((cP == NULL) || ((cP->topic = strdup(endpoint)) == NULL))
  {
    free(cP);
    pthread_mutex_unlock(&mtx);
    return BRIDGE_ERR;
  }

  cP->direction = direction;
  cP->qos       = qos;
  cP->next      = channels;
  channels      = cP;

  channelSubscribe(cP);       // a no-op until connected - onConnect subscribes everything then
  pthread_mutex_unlock(&mtx);

  return BRIDGE_OK;
}



// -----------------------------------------------------------------------------
//
// mqttChannelAdd - a host before ABI 9: no channelInfo
//
static int mqttChannelAdd(const char* endpoint, BridgeChannelKind kind, BridgeDirection direction)
{
  return mqttChannelAddInfo(endpoint, kind, direction, NULL);
}



// -----------------------------------------------------------------------------
//
// mqttChannelDel - stop carrying a topic
//
static int mqttChannelDel(const char* endpoint)
{
  pthread_mutex_lock(&mtx);

  MqttChannel** prevPP = &channels;
  for (MqttChannel* cP = channels; cP != NULL; prevPP = &cP->next, cP = cP->next)
  {
    if (strcmp(cP->topic, endpoint) != 0)
      continue;

    *prevPP = cP->next;
    if ((conn != NULL) && (cP->direction != BridgeDirectionOut))
      mosquitto_unsubscribe(conn, NULL, cP->topic);

    pthread_mutex_unlock(&mtx);
    free(cP->lastPublished);
    free(cP->topic);
    free(cP);
    return BRIDGE_OK;
  }

  pthread_mutex_unlock(&mtx);
  return BRIDGE_NOT_FOUND;
}



// -----------------------------------------------------------------------------
//
// mqttPublish - an attribute of a Channel was written: publish it on the topic
//
// Does not block: libmosquitto queues it for its own thread, which sends it when connected.
//
static int mqttPublish(const char* endpoint, const char* json)
{
  pthread_mutex_lock(&mtx);

  MqttChannel* cP = channelFind(endpoint);
  if ((cP == NULL) || (cP->direction == BridgeDirectionIn) || (conn == NULL))
  {
    pthread_mutex_unlock(&mtx);
    return BRIDGE_NOT_FOUND;
  }

  if ((connVersion != MQTT_PROTOCOL_V5) && (cP->direction == BridgeDirectionBoth))
  {
    free(cP->lastPublished);
    cP->lastPublished = strdup(json);
  }

  int rc = mosquitto_publish(conn, NULL, cP->topic, strlen(json), json, cP->qos, false);
  pthread_mutex_unlock(&mtx);

  if (rc != MOSQ_ERR_SUCCESS)
  {
    COR_W("mqtt: publish to '%s': %s", endpoint, mosquitto_strerror(rc));
    return BRIDGE_ERR;
  }

  return BRIDGE_OK;
}



// -----------------------------------------------------------------------------
//
// mqttInit -
//
// Notifications need no configuration - each Subscription carries its own server. The library is
// initialised here so a failure says so at startup, not on the first notification. Channels need
// the Bridge's server, mqtt.server in the bridge configuration.
//
static int mqttInit(const char* configFile, const BridgeBroker* _brokerP)
{
  brokerP = _brokerP;

  if (libInit() != 0)
  {
    COR_E("mqtt: mosquitto_lib_init failed");
    return BRIDGE_ERR;
  }

  //
  // A server configured: the Bridge's own connection, for its Channels. None: notifications only.
  //
  MqttUri u;
  char    clientId[128];
  int     version;
  bool    error;

  memset(&u, 0, sizeof(u));
  if (serverConfig(configFile, &u, clientId, sizeof(clientId), &version, &error) == false)
    return BRIDGE_OK;

  int r = (error == true) ? BRIDGE_ERR : connectionOpen(&u, clientId, version, false);

  free(u.buf);
  return r;
}



// -----------------------------------------------------------------------------
//
// mqttClose -
//
static void mqttClose(void)
{
  if (conn != NULL)
  {
    mosquitto_disconnect(conn);
    mosquitto_loop_stop(conn, false);   // returns once libmosquitto's thread - the one calling sampleIn - is gone
    mosquitto_destroy(conn);
    conn = NULL;
  }

  pthread_mutex_lock(&mtx);
  while (channels != NULL)
  {
    MqttChannel* cP = channels;

    channels = cP->next;
    free(cP->lastPublished);
    free(cP->topic);
    free(cP);
  }
  pthread_mutex_unlock(&mtx);

  if (libInitResult == 0)
    mosquitto_lib_cleanup();

  brokerP = NULL;
}



// -----------------------------------------------------------------------------
//
// mqttVersionInfo -
//
static const char* mqttVersionInfo(void)
{
  static char info[64];
  int         major, minor, revision;

  mosquitto_lib_version(&major, &minor, &revision);
  snprintf(info, sizeof(info), "mqtt 0.1.0 (libmosquitto %d.%d.%d)", major, minor, revision);

  return info;
}



// -----------------------------------------------------------------------------
//
// bridgeRegister - the one symbol the broker looks for
//
void bridgeRegister(BridgeDriver* driverP)
{
  if (driverP->abiVersion < 1)
    driverP->abiVersion = 1;                         // a host from before the handshake

  driverP->alias       = "mqtt";
  driverP->version     = "0.1.0";
  driverP->args        = NULL;
  driverP->init        = mqttInit;
  driverP->close       = mqttClose;
  driverP->versionInfo = mqttVersionInfo;
  driverP->channelAdd  = mqttChannelAdd;
  driverP->channelDel  = mqttChannelDel;
  driverP->publish     = mqttPublish;

  if (driverP->abiVersion >= 9)
    driverP->channelAddInfo = mqttChannelAddInfo;

  //
  // ABI 10 - only where the HOST has the slots: it allocated this struct at its own size
  //
  if (driverP->abiVersion >= 10)
  {
    driverP->notifySchemes = "mqtt,mqtts";
    driverP->notify        = mqttNotify;
  }

  driverP->abiVersion  = BRIDGE_ABI_VERSION;
}
