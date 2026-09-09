import { Consumer, EachMessagePayload, Kafka, KafkaConfig } from "kafkajs";
import { MessageBroker } from "../types/broker";
import ws from "../socket"
import config from "config"
import fs from "fs";

function buildSsl(): KafkaConfig["ssl"] {
    if (config.has("kafka.ssl.caPath")) {
        try {
            const caPath = config.get<string>("kafka.ssl.caPath");
            if (caPath) return { ca: [fs.readFileSync(caPath, "utf-8")] };
        } catch {
            // fall through to ssl:true (public CA, e.g. Confluent)
        }
    }
    return true;
}

function buildSasl(): KafkaConfig["sasl"] {
    const mechanism = config.has("kafka.sasl.mechanism")
        ? config.get<string>("kafka.sasl.mechanism")
        : "plain";
    const username = config.get<string>("kafka.sasl.username");
    const password = config.get<string>("kafka.sasl.password");
    if (mechanism === "scram-sha-256") return { mechanism: "scram-sha-256", username, password };
    if (mechanism === "scram-sha-512") return { mechanism: "scram-sha-512", username, password };
    return { mechanism: "plain", username, password };
}

export class KafkaBroker implements MessageBroker {
  private consumer: Consumer;

  constructor(clientId: string, brokers: string[]) {
    let kafkaConfig: KafkaConfig = { clientId, brokers };

    if (process.env.NODE_ENV === "production"){
            kafkaConfig = {
                ...kafkaConfig,
                ssl: buildSsl(),
                connectionTimeout: 450000,
                sasl: buildSasl()
            }
        }

    const kafka = new Kafka(kafkaConfig);
    this.consumer = kafka.consumer({ groupId: clientId });
  }

  /**
   * Connect the consumer
   */
  async connectConsumer() {
    await this.consumer.connect();
  }

  /**
   * Disconnect the consumer
   */
  async disconnectConsumer() {
    await this.consumer.disconnect();
  }

  async consumeMessage(topics: string[], fromBeginning: boolean = false) {
    await this.consumer.subscribe({ topics, fromBeginning });

    await this.consumer.run({
      eachMessage: async ({
        topic,
        partition,
        message,
      }: EachMessagePayload) => {
        // Logic to handle incoming messages.
        console.log({
          value: message.value.toString(),
          topic,
          partition,
        });

        switch(topic){
          case "order":{
            // todo: maybe we can check the event_type
             const order = JSON.parse(message.value.toString())
            ws.io.to(order.data.tenantId).emit("order-update", order)
          }
          break
          default:
            console.log("Doing nothing");
        }
      },
    });
  }
}
