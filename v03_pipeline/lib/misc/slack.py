from slacker import Slacker

from v03_pipeline.api.model import LoadingPipelineRequest
from v03_pipeline.lib.logger import get_logger
from v03_pipeline.lib.model import Env

SLACK_FAILURE_MESSAGE_PREFIX = ':failed: Pipeline Run Failed. :failed:'
SLACK_SUCCESS_MESSAGE_PREFIX = ':white_check_mark: Pipeline Run Success! Kicking off ClickHouse Load! :white_check_mark:'


logger = get_logger(__name__)


def safe_post_to_slack(message) -> None:
    try:
        if not Env.SLACK_TOKEN:
            logger.info(message)
            return
        slack = Slacker(Env.SLACK_TOKEN)
        slack.chat.post_message(
            Env.SLACK_NOTIFICATION_CHANNEL,
            message,
            as_user=False,
            icon_emoji=':beaker:',
            username='Beaker (engineering-minion)',
        )
    except Exception:
        logger.exception(
            f'Slack error:  Original message in channel ({Env.SLACK_NOTIFICATION_CHANNEL}) - {message}',
        )


def safe_post_to_slack_failure(run_id: str, lpr: LoadingPipelineRequest) -> None:
    message = '\n'.join(
        [
            SLACK_FAILURE_MESSAGE_PREFIX,
            f'Run ID: {run_id}',
            str(lpr),
        ],
    )
    safe_post_to_slack(message)


def safe_post_to_slack_success(run_id: str, lpr: LoadingPipelineRequest) -> None:
    message = '\n'.join(
        [
            SLACK_SUCCESS_MESSAGE_PREFIX,
            f'Run ID: {run_id}',
            str(lpr),
        ],
    )
    safe_post_to_slack(message)
