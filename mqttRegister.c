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
// v1 delivers NOTIFICATIONS: a Subscription whose notification.endpoint.uri is mqtt:// or mqtts://
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
#include <stdbool.h>                                  // bool
#include <stdio.h>                                    // snprintf
#include <stdlib.h>                                   // strtol, free
#include <string.h>                                   // strncmp, strchr, strrchr, strlen, strdup
#include <strings.h>                                  // strcasecmp
#include <pthread.h>                                  // pthread_once

#include <mosquitto.h>                                // mosquitto_*

#include "corLog/corLog.h"                            // COR_E, COR_W
#include "corAlloc/CorAlloc.h"                        // CorAlloc
#include "corAlloc/corAllocBufferInit.h"              // corAllocBufferInit
#include "corAlloc/corAllocBufferReset.h"             // corAllocBufferReset
#include "corJson/CorJson.h"                          // CorJson
#include "corJson/corJsonCreate.h"                    // corJsonCreate
#include "corJson/corJsonParse.h"                     // corJsonParse
#include "corTree/CorNode.h"                          // CorNode
#include "corTree/corTreeLookup.h"                    // corTreeLookup

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
// mqttInit -
//
// Notifications need no configuration - each Subscription carries its own server. The library is
// initialised here so a failure says so at startup, not on the first notification.
//
static int mqttInit(const char* configFile, const BridgeBroker* _brokerP)
{
  (void) configFile;

  brokerP = _brokerP;

  if (libInit() != 0)
  {
    COR_E("mqtt: mosquitto_lib_init failed");
    return BRIDGE_ERR;
  }

  return BRIDGE_OK;
}



// -----------------------------------------------------------------------------
//
// mqttClose -
//
static void mqttClose(void)
{
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

  //
  // ABI 10 - only where the HOST has the slots: it allocated this struct at its own size. An older
  // host has no notify(), and this plugin has nothing else to offer it yet.
  //
  if (driverP->abiVersion >= 10)
  {
    driverP->notifySchemes = "mqtt,mqtts";
    driverP->notify        = mqttNotify;
  }

  driverP->abiVersion  = BRIDGE_ABI_VERSION;
}
