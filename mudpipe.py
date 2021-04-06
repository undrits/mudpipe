#!/usr/bin/env python

"""
    Parallelize UDPipe processing with Python Multiprocessing.
    Requires: UDPipe, xz, xzcat

    Example:
        $ ./mudpipe.py \
                --input-xz \
                --output-xz \
                --arg='--tokenize' \
                --arg='--tag' \
                --arg='--parse' \
                --model=./russian-syntagrus-ud-2.5-191206.udpipe \
                --path_dir=./data/ru.*.txt.xz
                --workers=4

"""


import argparse
import glob
import itertools
import multiprocessing as mp
import subprocess as sp
from typing import List


def process(
    arguments: List, model: str, file: str, input_xz: bool, output_xz: bool
) -> None:

    # initial setup
    call_args = ["udpipe", "--immediate"]
    call_args.extend(arguments)
    call_args.append(model)

    # process xz inputs and save the output in xz format
    if input_xz:
        if output_xz:
            xzcat = sp.Popen(
                ["xzcat", file],
                stdout=sp.PIPE,
            )
            udpipe = sp.Popen(
                call_args,
                stdin=xzcat.stdout,
                stdout=sp.PIPE,
            )
            with open(f"{file}.conllu.xz", "w") as outfile:
                xz = sp.Popen(
                    "xz",
                    stdin=udpipe.stdout,
                    stdout=outfile,
                )
                xz.communicate()
        # process xz inputs and save in conllu format
        else:
            save_arg = f"--outfile={file}.conllu"
            call_args.append(save_arg)
            xzcat = sp.Popen(
                ["xzcat", file],
                stdout=sp.PIPE,
            )

            udpipe = sp.Popen(
                call_args,
                stdin=xzcat.stdout
            )
            udpipe.communicate()

    # process and save in xz format
    elif output_xz:
        call_args.append(file)
        udpipe = sp.Popen(
            call_args,
            stdout=sp.PIPE,
        )
        with open(f"{file}.conllu.xz", "w") as outfile:
            xz = sp.Popen(
                "xz",
                stdin=udpipe.stdout,
                stdout=outfile,
            )
            xz.communicate()

    # process and save in conllu format
    else:
        save_arg = f"--outfile={file}.conllu"
        call_args.append(save_arg)
        call_args.append(file)
        sp.check_call(call_args)


def main(args: argparse.Namespace):

    # create tasks
    tasks = zip(
        itertools.repeat(args.arg),
        itertools.repeat(args.model),
        glob.iglob(args.path_dir),
        itertools.repeat(args.input_xz),
        itertools.repeat(args.output_xz),
    )

    # run
    if args.workers:
        count = args.workers
    else:
        count = mp.cpu_count()
    with mp.Pool(processes=count) as pool:
        if args.chunksize:
            pool.starmap(process, tasks, chunksize=args.chunksize)
        else:
            pool.starmap(process, tasks)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--arg",
        required=True,
        help="argument(s) to pass (start with --)",
        action="append",
    )
    parser.add_argument(
        "--model",
        required=True,
        help="udpipe model",
    )
    parser.add_argument(
        "--path_dir",
        required=True,
        help="path to the folder with the files",
    )
    parser.add_argument(
        "--chunksize",
        help="number of chunks (can be specified for very long iterables)",
        type=int
    )
    parser.add_argument(
        "--workers",
        help="number of workers",
        type=int
    )
    parser.add_argument(
        "--input-xz",
        help="enable xz input",
        action="store_true"
    )
    parser.add_argument(
        "--output-xz",
        help="enable xz output",
        action="store_true"
    )
    main(parser.parse_args())
