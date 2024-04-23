from aws_lambda_powertools import Logger, Tracer


service_name = "DeepRacerSidecarProcess"
tracer = Tracer(service=service_name)

logger = Logger(service=service_name, log_uncaught_exceptions=True, level="INFO")
