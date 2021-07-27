#!/usr/bin/env python3
"""
the requestor agent controlling and interacting with the "ftse service"
"""
import asyncio
from datetime import datetime, timedelta, timezone

from yapapi import (
    NoPaymentAccountError,
    __version__ as yapapi_version,
    windows_event_loop_fix,
)
from yapapi import Golem
from yapapi.services import Service, ServiceState

from yapapi.log import enable_default_logger, pluralize
from yapapi.payload import vm

from utils import (
    build_parser,
    TEXT_COLOR_DEFAULT,
    TEXT_COLOR_RED,
    TEXT_COLOR_YELLOW,
)

NUM_INSTANCES = 1
STARTING_TIMEOUT = timedelta(minutes=4)

class FtseService(Service):
    FTSE_SERVICE = "/golem/run/ftse.py"

    @staticmethod
    async def get_payload():
        return await vm.repo(
            image_hash="ec8e6cf20c6ba8d78ad0db23ccfcc5155586e88ca798caa6924328d2",
        )

    async def start(self):
        # handler responsible for starting the service
        self._ctx.send_file("./service/data/testfile1.txt", "/golem/in/testfile1.txt")
        self._ctx.send_file("./service/data/testfile2.txt", "/golem/in/testfile2.txt")
        self._ctx.send_file("./service/data/testfile3.txt", "/golem/in/testfile3.txt")
        self._ctx.run(self.FTSE_SERVICE, "--init")
        future_results = yield self._ctx.commit()
        results = await future_results
        for x in range(len(results)):
            print(f"Result: {x}")
            print(results[x].stdout)
        print("Start finished")
        print("--------------")

    async def run(self):
        # handler responsible for providing the required interactions while the service is running
        while True:
            search_term = input("Enter a search term: ")
            self._ctx.run(self.FTSE_SERVICE, "--search", search_term)

            future_results = yield self._ctx.commit()
            results = await future_results
            for x in range(len(results)):
                print(results[x].stdout)

    async def shutdown(self):
        # handler reponsible for executing operations on shutdown
        yield self._ctx.commit()            


async def main(subnet_tag, running_time, driver=None, network=None):
    async with Golem(
        budget=1.0,
        subnet_tag=subnet_tag,
        driver=driver,
        network=network,
    ) as golem:

        print(
            f"yapapi version: {TEXT_COLOR_YELLOW}{yapapi_version}{TEXT_COLOR_DEFAULT}\n"
            f"Using subnet: {TEXT_COLOR_YELLOW}{subnet_tag}{TEXT_COLOR_DEFAULT}, "
            f"payment driver: {TEXT_COLOR_YELLOW}{golem.driver}{TEXT_COLOR_DEFAULT}, "
            f"and network: {TEXT_COLOR_YELLOW}{golem.network}{TEXT_COLOR_DEFAULT}\n"
        )

        commissioning_time = datetime.now()

        print(
            f"{TEXT_COLOR_YELLOW}"
            f"Starting {pluralize(NUM_INSTANCES, 'instance')}..."
            f"{TEXT_COLOR_DEFAULT}"
        )

        # start the service

        cluster = await golem.run_service(
            FtseService,
            num_instances=NUM_INSTANCES,
            expiration=datetime.now(timezone.utc) + timedelta(minutes=120),
        )

        # helper functions to display / filter instances

        def instances():
            return [(s.provider_name, s.state.value) for s in cluster.instances]

        def still_running():
            return any([s for s in cluster.instances if s.is_available])

        def still_starting():
            return len(cluster.instances) < NUM_INSTANCES or any(
                [s for s in cluster.instances if s.state == ServiceState.starting]
            )

        # wait until instances are started

        while still_starting() and datetime.now() < commissioning_time + STARTING_TIMEOUT:
            print(f"instances: {instances()}")
            await asyncio.sleep(5)

        if still_starting():
            raise Exception(f"Failed to start instances before {STARTING_TIMEOUT} elapsed :( ...")

        print(f"{TEXT_COLOR_YELLOW}All instances started :){TEXT_COLOR_DEFAULT}")

        # allow the service to run for a short while
        # (and allowing its requestor-end handlers to interact with it)

        start_time = datetime.now()

        while datetime.now() < start_time + timedelta(seconds=running_time):
            print(f"instances: {instances()}")
            await asyncio.sleep(5)

        print(f"{TEXT_COLOR_YELLOW}Stopping instances...{TEXT_COLOR_DEFAULT}")
        cluster.stop()

        # wait for instances to stop

        cnt = 0
        while cnt < 10 and still_running():
            print(f"instances: {instances()}")
            await asyncio.sleep(5)

    print(f"instances: {instances()}")


if __name__ == "__main__":
    parser = build_parser(
        "A full-text search engine service"
    )
    parser.add_argument(
        "--running-time",
        default=120,
        type=int,
        help=(
            "How long should the instance run before the cluster is stopped "
            "(in seconds, default: %(default)s)"
        ),
    )
    now = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    parser.set_defaults(log_file=f"ftse-{now}.log")
    args = parser.parse_args()

    # This is only required when running on Windows with Python prior to 3.8:
    windows_event_loop_fix()

    enable_default_logger(
        log_file=args.log_file,
        debug_activity_api=True,
        debug_market_api=True,
        debug_payment_api=True,
    )

    loop = asyncio.get_event_loop()
    task = loop.create_task(
        main(
            subnet_tag=args.subnet_tag,
            running_time=args.running_time,
            driver=args.driver,
            network=args.network,
        )
    )

    try:
        loop.run_until_complete(task)
    except NoPaymentAccountError as e:
        handbook_url = (
            "https://handbook.golem.network/requestor-tutorials/"
            "flash-tutorial-of-requestor-development"
        )
        print(
            f"{TEXT_COLOR_RED}"
            f"No payment account initialized for driver `{e.required_driver}` "
            f"and network `{e.required_network}`.\n\n"
            f"See {handbook_url} on how to initialize payment accounts for a requestor node."
            f"{TEXT_COLOR_DEFAULT}"
        )
    except KeyboardInterrupt:
        print(
            f"{TEXT_COLOR_YELLOW}"
            "\nShutting down gracefully, please wait a short while "
            "or press Ctrl+C to exit immediately..."
            f"{TEXT_COLOR_DEFAULT}"
        )
        task.cancel()
        try:
            loop.run_until_complete(task)
            print(
                f"{TEXT_COLOR_YELLOW}Shutdown completed, thank you for waiting!{TEXT_COLOR_DEFAULT}"
            )
        except (asyncio.CancelledError, KeyboardInterrupt):
            pass