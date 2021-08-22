#!/usr/bin/env python3

import asyncio
import sys
import shlex

from yapapi import (
    NoPaymentAccountError,
    __version__ as yapapi_version
)

from yapapi.payload import vm
from yapapi.services import Service

from yapapi_service_manager import ServiceManager

from utils import (
    TEXT_COLOR_DEFAULT,
    TEXT_COLOR_RED,
    TEXT_COLOR_YELLOW,
)

class FtseService(Service):
    FTSE_SERVICE = "/golem/run/ftse.py"

    @classmethod
    async def get_payload(cls):
        image_hash = "ec8e6cf20c6ba8d78ad0db23ccfcc5155586e88ca798caa6924328d2"
        return await vm.repo(image_hash=image_hash)

    async def start(self):
        # handler responsible for starting the service
        self._ctx.send_file("./service/data/testfile1.txt", "/golem/in/testfile1.txt")
        self._ctx.send_file("./service/data/testfile2.txt", "/golem/in/testfile2.txt")
        self._ctx.send_file("./service/data/testfile3.txt", "/golem/in/testfile3.txt")
        self._ctx.run(self.FTSE_SERVICE, "--init")
        future_results = yield self._ctx.commit()
        await future_results
        print("Start finished")
        print("--------------")

    async def run(self):
        while True:
            print('Enter a search term:')
            signal = await self._listen()
            cmd = signal.message
            cmd = shlex.quote(cmd)
            self._ctx.run(self.FTSE_SERVICE, "--search", cmd)

            future_results = yield self._ctx.commit()
            results = await future_results
            for x in range(len(results)):
                print(results[x].stdout) 


async def async_stdin_reader():
    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)
    return reader


async def run_service(service_manager):
    svc = service_manager.create_service(FtseService)

    while svc.status != 'running':
        print(f"Service is not running yet. Current state: {svc.status}")
        await asyncio.sleep(3)

    reader = await async_stdin_reader()
    while True:
        line = await reader.readline()
        line = line.decode()
        svc.service.send_message_nowait(line)


def main():
    executor_cfg = {
        'subnet_tag': 'devnet-beta.2',
        'budget': 1,
    }

    print(
        f"yapapi version: {TEXT_COLOR_YELLOW}{yapapi_version}{TEXT_COLOR_DEFAULT}\n"
        f"Using subnet: {TEXT_COLOR_YELLOW}{executor_cfg['subnet_tag']}{TEXT_COLOR_DEFAULT} "
        f"and budget: {TEXT_COLOR_YELLOW}{executor_cfg['budget']}{TEXT_COLOR_DEFAULT}\n"
    )

    service_manager = ServiceManager(executor_cfg)
    try:
        loop = asyncio.get_event_loop()
        run_service_task = loop.create_task(run_service(service_manager))
        loop.run_until_complete(run_service_task)
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
            "\nShutting down gracefully, please wait a short while..."
            f"{TEXT_COLOR_DEFAULT}"
        )        
        shutdown = loop.create_task(service_manager.close())
        loop.run_until_complete(shutdown)
        run_service_task.cancel()


if __name__ == '__main__':
    main()