import json
import pytest

from aws_cdk import core
from infra.infra_stack import InfraStack


def get_template():
    app = core.App()
    InfraStack(app, "infra")
    return json.dumps(app.synth().get_stack("infra").template)


def test_sqs_queue_created():
    assert("AWS::SQS::Queue" in get_template())


def test_sns_topic_created():
    assert("AWS::SNS::Topic" in get_template())
