# corMqttBridge

The **MQTT bridge** for coraine: an `mqtt.so` that carries NGSI-LD over MQTT,
through libmosquitto. Until this repo, libmosquitto was linked into every broker;
now a broker started without `--bridges mqtt` never maps it.

## What it does - v1: notifications

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

**Next:** MQTT Channels - a topic ↔ an entity attribute, in both directions, as
the DDS and Modbus bridges do. Their `channelInfo` will use the same keys a
Subscription uses (`MQTT-QoS`, `MQTT-Version`).

## The contract

`corBridge` holds it: `BridgeDriver.h` is what this fills in - for notifications
`notifySchemes` and `notify()`, ABI 10.

## Building

Needs libmosquitto (Debian/Ubuntu: `libmosquitto-dev`).

    make                  # mqtt.so, debug (traces compiled in)
    make BUILD=release
    make install          # to /opt/seamware/plugins/bridge (PLUGIN_DIR)
    make contract         # does it still satisfy BridgeDriver.h?

The sibling cor repos are expected beside this one (`COR_LIBS ?= ..`); the
corLibs umbrella clones and builds it with the rest. Its tests live in coraine:
a bridge is tested through the broker that loads it.
