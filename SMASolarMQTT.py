#!/usr/bin/python


# GNU GENERAL PUBLIC LICENSE -  Version 2, June 1991
# See LICENCE and README file for details

# This will background the task
# nohup python SMASolarMQTT.py 00:80:25:1D:AC:53 0000 1> SMASolarMQTT.log 2> SMASolarMQTT.log.error &

__author__ = 'Stuart Pittaway'

import time
import argparse
import sys
import paho.mqtt.client as mqtt
import bluetooth
import datetime
from datetime import datetime

from SMANET2PlusPacket import SMANET2PlusPacket
from SMABluetoothPacket import SMABluetoothPacket
import SMASolarMQTT_library

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    #print("Connected to MQTT with result code "+str(rc))
    pass

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    #print(msg.topic+" "+str(msg.payload))
    pass

def main(bd_addr, InverterPassword, mqtt_server, mqtt_user, mqtt_pass, mqtt_topic):
    InverterCodeArray = bytearray([0x5c, 0xaf, 0xf0, 0x1d, 0x50, 0x00]);

    # Dummy arrays
    AddressFFFFFFFF = bytearray([0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
    Address00000000 = bytearray([0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
    InverterPasswordArray = SMASolarMQTT_library.encodeInverterPassword(InverterPassword)
    port = 1

    error_count = 0
    packet_send_counter = 0

    try:

        # Connect to MQTT
        client = mqtt.Client()
        client.on_connect = on_connect
        client.on_message = on_message
        client.username_pw_set(username=mqtt_user,password=mqtt_pass)
        client.connect(mqtt_server, 1883, 60)

        #print "Connecting to SMA Inverter over Bluetooth"
        btSocket = bluetooth.BluetoothSocket(bluetooth.RFCOMM)
        btSocket.connect((bd_addr, port))
        # Give BT 5 seconds to timeout so we don't hang and wait forever
        btSocket.settimeout(10)

        # http://pybluez.googlecode.com/svn/www/docs-0.7/public/bluetooth.BluetoothSocket-class.html
        mylocalBTAddress = SMASolarMQTT_library.BTAddressToByteArray(btSocket.getsockname()[0], ":")
        mylocalBTAddress.reverse()
        # LogMessageWithByteArray("mylocalBTAddress", mylocalBTAddress)

        SMASolarMQTT_library.initaliseSMAConnection(btSocket, mylocalBTAddress, AddressFFFFFFFF, InverterCodeArray,
                                                    packet_send_counter)

        # Logon to inverter
        pluspacket1 = SMASolarMQTT_library.SMANET2PlusPacket(0x0e, 0xa0, packet_send_counter, InverterCodeArray,
                                                             0x00, 0x01, 0x01)
        pluspacket1.pushRawByteArray(
            bytearray([0x80, 0x0C, 0x04, 0xFD, 0xFF, 0x07, 0x00, 0x00, 0x00, 0x84, 0x03, 0x00, 0x00]))
        pluspacket1.pushRawByteArray(
            SMASolarMQTT_library.floattobytearray(SMASolarMQTT_library.time.mktime(datetime.today().timetuple())))
        pluspacket1.pushRawByteArray(bytearray([0x00, 0x00, 0x00, 0x00]))
        pluspacket1.pushRawByteArray(InverterPasswordArray)

        send = SMASolarMQTT_library.SMABluetoothPacket(1, 1, 0x00, 0x01, 0x00, mylocalBTAddress, AddressFFFFFFFF)
        send.pushRawByteArray(pluspacket1.getBytesForSending())
        send.finish()
        send.sendPacket(btSocket)

        bluetoothbuffer = SMASolarMQTT_library.read_SMA_BT_Packet(btSocket, packet_send_counter, True,
                                                                  mylocalBTAddress)

        SMASolarMQTT_library.checkPacketReply(bluetoothbuffer, 0x0001)

        packet_send_counter = packet_send_counter + 1

        if bluetoothbuffer.leveltwo.errorCode() > 0:
            raise Exception("Error code returned from inverter - during logon - wrong password?")

        inverterserialnumber = bluetoothbuffer.leveltwo.getFourByteLong(16)
        invName = SMASolarMQTT_library.getInverterName(btSocket, packet_send_counter, mylocalBTAddress,
                                                       InverterCodeArray, AddressFFFFFFFF)
        # MQTT Blocking call that processes network traffic, dispatches callbacks and handles reconnecting.
        client.loop()

        packet_send_counter = packet_send_counter + 1

        L2 = SMASolarMQTT_library.spotvalues_ac(btSocket, packet_send_counter, mylocalBTAddress,
                                                InverterCodeArray, AddressFFFFFFFF)
        # Output 10 parameters for AC values
        # 0x4640 AC Output Phase 1
        # 0x4641 AC Output Phase 2
        # 0x4642 AC Output Phase 3
        # 0x4648 AC Line Voltage Phase 1
        # 0x4649 AC Line Voltage Phase 2
        # 0x464a AC Line Voltage Phase 3
        # 0x4650 AC Line Current Phase 1
        # 0x4651 AC Line Current Phase 2
        # 0x4652 AC Line Current Phase 3
        # 0x4657 AC Grid Frequency
        client.publish(mqtt_topic + "/ACOutputPhase1", payload=L2[1][1], qos=0, retain=False)
        client.publish(mqtt_topic + "/ACOutputPhase2", payload=L2[2][1], qos=0, retain=False)
        client.publish(mqtt_topic + "/ACOutputPhase3", payload=L2[3][1], qos=0, retain=False)
        client.publish(mqtt_topic + "/ACLineVoltagePhase1", payload=L2[4][1], qos=0, retain=False)
        client.publish(mqtt_topic + "/ACLineVoltagePhase2", payload=L2[5][1], qos=0, retain=False)
        client.publish(mqtt_topic + "/ACLineVoltagePhase3", payload=L2[6][1], qos=0, retain=False)
        client.publish(mqtt_topic + "/ACLineCurrentPhase1", payload=L2[7][1], qos=0, retain=False)
        client.publish(mqtt_topic + "/ACLineCurrentPhase2", payload=L2[8][1], qos=0, retain=False)
        client.publish(mqtt_topic + "/ACLineCurrentPhase3", payload=L2[9][1], qos=0, retain=False)
        client.publish(mqtt_topic + "/ACLineGridFrequency", payload=L2[10][1], qos=0, retain=False)
        time.sleep(1)

        packet_send_counter = packet_send_counter + 1

        L2 = SMASolarMQTT_library.spotvalues_actotal(btSocket, packet_send_counter, mylocalBTAddress,
                                                     InverterCodeArray, AddressFFFFFFFF)
        # #0x263f AC Power Watts Total
        client.publish(mqtt_topic + "/ACOutputTotal", payload=L2[1][1], qos=0, retain=False)
        time.sleep(1)

        packet_send_counter = packet_send_counter + 1

        L2 = SMASolarMQTT_library.spotvalues_dcwatts(btSocket, packet_send_counter, mylocalBTAddress,
                                                     InverterCodeArray, AddressFFFFFFFF)
        # #0x251e DC Power Watts
        client.publish(mqtt_topic + "/String1_DCWatts", payload=L2[1][1], qos=0, retain=False)
        client.publish(mqtt_topic + "/String2_DCWatts", payload=L2[2][1], qos=0, retain=False)
        time.sleep(1)

        packet_send_counter = packet_send_counter + 1

        L2 = SMASolarMQTT_library.spotvalues_yield(btSocket, packet_send_counter, mylocalBTAddress,
                                                   InverterCodeArray, AddressFFFFFFFF)
        # 0x2601 Total Yield kWh
        # 0x2622 Day Yield kWh
        # 0x462e Operating time (hours)
        # 0x462f Feed in time (hours)
        client.publish(mqtt_topic + "/TotalYield", payload=L2[1][1], qos=0, retain=False)
        client.publish(mqtt_topic + "/DayYield", payload=L2[2][1], qos=0, retain=False)
        client.publish(mqtt_topic + "/OperatingTime", payload=L2[3][1], qos=0, retain=False)
        client.publish(mqtt_topic + "/FeedInTime", payload=L2[4][1], qos=0, retain=False)
        time.sleep(1)

        packet_send_counter = packet_send_counter + 1

        #0x451f DC Voltage V
        #0x4521 DC Current A
        L2 = SMASolarMQTT_library.spotvalues_dc(btSocket, packet_send_counter, mylocalBTAddress,
                                                InverterCodeArray, AddressFFFFFFFF)
        client.publish(mqtt_topic + "/String1_DCVoltage", payload=L2[1][1], qos=0, retain=False)
        client.publish(mqtt_topic + "/String2_DCVoltage", payload=L2[2][1], qos=0, retain=False)
        client.publish(mqtt_topic + "/String1_DCCurrent", payload=L2[3][1], qos=0, retain=False)
        client.publish(mqtt_topic + "/String2_DCCurrent", payload=L2[4][1], qos=0, retain=False)
        time.sleep(1)

    except bluetooth.btcommon.BluetoothError as inst:
        btSocket.close()
	print("BT Error")

    except Exception as inst:
        btSocket.close()
	print("Exception")
	print inst.args


parser = argparse.ArgumentParser(
    description='Report generation statistics from SMA photovoltaic inverter over a shared MQTT hub.  Designed for use with emonCMS and emonPi see http://openenergymonitor.org/emon/',
    epilog='Copyright 2013-2015 Stuart Pittaway.')

parser.add_argument('addr', metavar='addr', type=str,
                    help='Bluetooth address of SMA inverter in 00:80:22:11:cc:55 format, run hcitool scan to find yours.')

parser.add_argument('passcode', metavar='passcode', type=str,
                    help='NUMERIC pass code for the inverter, default of 0000.')

parser.add_argument('mqttserver', metavar='mqttserver', type=str,
                    help='MQTT host.')

parser.add_argument('mqttuser', metavar='mqttuser', type=str,
                    help='MQTT user.')

parser.add_argument('mqttpass', metavar='mqttpass', type=str,
                    help='MQTT pass.')

parser.add_argument('mqtttopic', metavar='mqtttopic', type=str,
                    help='MQTT Topic.')

args = parser.parse_args()

main(args.addr, args.passcode, args.mqttserver, args.mqttuser, args.mqttpass, args.mqtttopic)

exit()
