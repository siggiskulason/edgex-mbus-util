package main

import (
	"fmt"
	"github.com/edgexfoundry/go-mod-messaging/messaging"
	"github.com/edgexfoundry/go-mod-messaging/pkg/types"
)

func main() {
	fmt.Println("edgex-mbus-util")
	zero:= connectZero()
 	testClient(zero)

	mqtt:= connectMQTT()
	testClient(mqtt)

}


func publishEvent(expectedCorreleationID string ,expectedPayload[] byte, topic string, client messaging.MessageClient) {
	msgEnvelope := types.MessageEnvelope{
		CorrelationID: expectedCorreleationID,
		Payload:       expectedPayload,
		ContentType:   "application/json",
	}
	err := client.Publish(msgEnvelope, topic)
	if err != nil {
		fmt.Println("failed to  publish : " + err.Error())
	}
}
func subscribeToEvents(topic string, client messaging.MessageClient) (chan types.MessageEnvelope, chan error) {
	messages := make(chan types.MessageEnvelope)
	messageErrors := make(chan error)

	topics := []types.TopicChannel{{Topic: topic, Messages: messages}}

	err := client.Subscribe(topics, messageErrors)
	if err != nil {
		fmt.Println("failed to subscribe for event messages: " + err.Error())
	}
	return messages,messageErrors
}
const (
	zeromqPort = 5570
	mqttPort = 1883
)

func connectZero()  messaging.MessageClient {
	msgConfig := types.MessageBusConfig{
		PublishHost: types.HostInfo{
			Host:     "*",
			Port:     zeromqPort,
			Protocol: "tcp",
		},
		SubscribeHost: types.HostInfo{
			Host:     "localhost",
			Port:     zeromqPort,
			Protocol: "tcp",
		},
		Type: "zero",
	}
	client := connect(msgConfig)
	return client
}

func connectMQTT()  messaging.MessageClient {
	msgConfig := types.MessageBusConfig{
		PublishHost: types.HostInfo{
			Host:     "*",
			Port:     mqttPort,
			Protocol: "tcp",
		},
		SubscribeHost: types.HostInfo{
			Host:     "localhost",
			Port:     mqttPort,
			Protocol: "tcp",
		},
		Type: "mqtt",
	}
	client := connect(msgConfig)
	return client
}



func testClient(client messaging.MessageClient ) {
	expectedCorreleationID := "123"
	expectedPayload := []byte("\"answer\":\"42\",\"question\":\"what is 6x7\"")
	topic :="test"

	messages, messageErrors := subscribeToEvents(topic, client )
	publishEvent(expectedCorreleationID,expectedPayload, topic, client )

	select {
	case e := <-messageErrors:
		// handle errors
		fmt.Println("failure: " + e.Error())

	case msgEnvelope := <- messages:
		str := string(msgEnvelope.Payload)
		fmt.Printf("Event received on message queue Correlation-id: %s : %s ",   msgEnvelope.CorrelationID,str)
	}
}

func connect(cfg types.MessageBusConfig) messaging.MessageClient {
 
 
	client, err := messaging.NewMessageClient(cfg)

	if err != nil {
		fmt.Println("failed to create messaging client: " + err.Error())
	}

	err = client.Connect()

	if err != nil {
		fmt.Println("failed to connect to message bus: " + err.Error())
	}

 	return client;
	
}
