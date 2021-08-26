from typing_extensions import runtime
from aws_cdk.aws_lambda_event_sources import S3EventSource
from aws_cdk import (
    aws_iam as iam,
    aws_sqs as sqs,
    aws_sns as sns,
    aws_s3 as s3,
    aws_sns_subscriptions as subs,
    aws_dynamodb as dynamodb,
    aws_lambda as _lambda,
    core
)


class InfraStack(core.Stack):

    def __init__(self, scope: core.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        sink_table = dynamodb.Table(
            self, "sink_table",
            partition_key=dynamodb.Attribute(
                name="id",
                type=dynamodb.AttributeType.STRING
            )
        )

        source_bucket = s3.Bucket(self, "source-bucket-auckland-2021",
                                    versioned=False,
                                    removal_policy=core.RemovalPolicy.DESTROY,
                                    bucket_name="source-bucket-auckland-2021")

        etl_lambda = _lambda.Function(
            self, 'ETLHandler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            code=_lambda.Code.asset('../etl'),
            handler='etl.handler',
            environment={
              'TABLE_NAME': 'abc'
            }
        )

        source_bucket.grant_read_write(etl_lambda)
        etl_lambda.add_event_source(S3EventSource(source_bucket,
                                    events=[s3.EventType.OBJECT_CREATED]
                    ))

        sink_table.grant_read_write_data(etl_lambda)
