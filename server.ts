import logger from "./src/config/logger";
import { createMessageBroker } from "./src/factories/broker-factory";

const startServer = async () => {
  try {
    const broker = createMessageBroker();
    await broker.connectConsumer();
    await broker.consumeMessage(["order"], false);
  } catch (err) {
    logger.error("Error happened: ", err.message);
    process.exit(1);
  }
};

void startServer();
