def notify_start_training():
    if 'SNS_TOPIC_ARN' not in os.environ or 'SNS_ACCESS_KEY_ID' not in os.environ or 'SNS_SECRET_ACCESS_KEY' not in os.environ or 'SNS_SESSION_TOKEN' not in os.environ:
        return
    sns_topic_arn = os.environ['SNS_TOPIC_ARN']
    sns_access_key_id = os.environ['SNS_ACCESS_KEY_ID']
    sns_secret_access_key = os.environ['SNS_SECRET_ACCESS_KEY']
    sns_session_token = os.environ['SNS_SESSION_TOKEN']
    session = boto3.Session(
        aws_access_key_id=sns_access_key_id,
        aws_secret_access_key=sns_secret_access_key,
        aws_session_token=sns_session_token
    )
    sns = session.client('sns')
    data = {
        'message_type': 'SIMULATION_WORKER_START',
        'sim_id': int(os.environ['SIMULATION_ID']),
        'rollout_idx': int(os.environ['ROLLOUT_IDX']),
        'date_time': datetime.utcnow().isoformat()
    }
    message = json.dumps(data)
    sns.publish(
        TopicArn=sns_topic_arn,
        Message=message
    )