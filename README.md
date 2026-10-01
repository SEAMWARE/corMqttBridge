# corMqttBridge

The **MQTT bridge** for coraine: an `mqtt.so` that carries NGSI-LD over MQTT,
through libmosquitto. Until this repo, libmosquitto was linked into every broker;
now a broker started without `--bridges mqtt` never maps it.

## Notifications

A Subscription whose `notification.endpoint.uri` is `mqtt://` or `mqtts://`
(TS 104 243, *NGSI-LD MQTT Notification Binding*) is delivered by this plugin.
Everything about the delivery is in the Subscription, as NGSI-LD defines it:

| Where | What |
|---|---|
| `endpoint.uri` | `mqtt[s]://[<user>[:<pass>]@]<host>[:<port>]/<topic>[/<subtopic>]*` - default ports 1883 / 8883 |
| `endpoint.notifierInfo` | `MQTT-QoS` (`0`, `1`, `2` - default `0`) and `MQTT-Version` (`mqtt3.1.1`, `mqtt5.0` - default `mqtt5.0`) |
| `endpoint.receiverInfo` | copied into the message's `metadata` by the broker |

The message - the `{ "metadata": ..., "body": ... }` envelope - is built by the
broker; this plugin sends it as it is given. With `--insecureNotif`, an `mqtts://`
endpoint's self-signed certificate is accepted.

Without `--bridges mqtt`, the broker refuses such a Subscription when it is
created or updated, rather than accepting it and failing every notification.

## Channels - a topic <-> an entity attribute

As for every bridge: a message on a Channel's topic becomes the attribute's
value, and a write to the attribute is published on the topic. The Bridge is one
connection, to `mqtt.server` in the `--bridgeConfig` file - a Subscription's MQTT
URI without the topic, plus the connection's `MQTT-Version`:

```json
{ "mqtt": { "server": { "uri": "mqtt://mosquitto:1883", "MQTT-Version": "mqtt5.0", "clientId": "coraine-plant" },
            "ngsild": { "topics": {
              "plant/tank1/level": { "entityId": "urn:ngsi-ld:Tank:T1", "entityType": "Tank", "attribute": "level",
                                     "channelInfo": [ { "key": "MQTT-QoS", "value": "1" } ] } } } } }
```

- A Channel's topic is exact - no `+` / `#`: a message finds its Channel by it.
- `channelInfo` uses `notifierInfo`'s vocabulary: `MQTT-QoS`.
- A JSON payload is the value as it is; anything else becomes a JSON string.
- The bridge's own publications do not come back as samples (MQTT 5 *no local*;
  on 3.1.1 the last payload published is dropped once when it echoes).
- libmosquitto's threaded loop owns the connection: started in the background,
  reconnecting by itself, every inbound topic subscribed again on reconnect.

The user documentation is coraine's
[doc/mqtt-bridge.md](https://github.com/SEAMWARE/coraine/blob/main/doc/mqtt-bridge.md).

## The contract

`corBridge` holds it: `BridgeDriver.h` is what this fills in - for notifications
`notifySchemes` and `notify()` (ABI 10), and for Channels `channelAddInfo`,
`channelDel` and `publish()`, with `sampleIn()` on the broker's side.

## Building

Needs libmosquitto (Debian/Ubuntu: `libmosquitto-dev`).

    make                  # mqtt.so, debug (traces compiled in)
    make BUILD=release
    make install          # to /opt/seamware/plugins/bridge (PLUGIN_DIR)
    make contract         # does it still satisfy BridgeDriver.h?

The sibling cor repos are expected beside this one (`COR_LIBS ?= ..`); the
corLibs umbrella clones and builds it with the rest. Its tests live in coraine:
a bridge is tested through the broker that loads it.
