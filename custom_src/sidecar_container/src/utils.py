from aws_lambda_powertools import Logger, Tracer


service_name = "DeepRacerSidecarContainer"
tracer = Tracer(service=service_name)

logger = Logger(log_uncaught_exceptions=True, level="INFO")
