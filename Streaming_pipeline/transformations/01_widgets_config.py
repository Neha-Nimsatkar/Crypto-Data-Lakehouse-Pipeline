# Cell 1: Creating UI Input Boxes (Widgets) inside your Serverless Notebook
dbutils.widgets.text("kafka_bootstrap_server", "", "1. Kafka Bootstrap Server")
dbutils.widgets.text("kafka_api_key", "", "2. Kafka API Key")
dbutils.widgets.text("kafka_api_secret", "", "3. Kafka API Secret")

print(" Look at the very top of your notebook! You will see 3 text boxes.")
