#!/usr/bin/env python3

from dataclasses import dataclass
import csv
import argparse
from datetime import datetime, timedelta
from typing import Optional
from sys import stderr
import re

# === CONSTANTS ===

# IMAGE CONSTANTS
PHOTOS_DURING_RDA = 8
BITS_PER_PIXEL = 10
IMAGE_SIZE = 2048 * 2048
RDA_ACQUIRED_DATA = PHOTOS_DURING_RDA * BITS_PER_PIXEL * IMAGE_SIZE
LOSSY_COMPRESSION_RATE = 8
LOSSLESS_COMPRESSION_RATE = 1.7

# TELEMETRY CONSTANTS
SUMMARY_FRAME_SIZE = 1400
SUMMARY_COLLECTION_PERIOD_S = 10
DETAILED_FRAME_SIZE = 1400
DETAILED_COLLECTION_PERIOD_S = 10

# TRANSMISSIONS CONSTANTS
ENDURO_PROT_OVERHEAD = 1.3 * 1.2  # 1.7
FEC_RATE = 7 / 8
COMM_OVERHEAD = ENDURO_PROT_OVERHEAD * (1 / FEC_RATE)
SBAND_BPS_BASE = 100 * 10**3
SBAND_BPS = SBAND_BPS_BASE / COMM_OVERHEAD
UHF_BPS_BASE = 9600
UHF_BPS = UHF_BPS_BASE / COMM_OVERHEAD


# OPERATIONAL MODE NAMES
E_START_SIMULATION = "start_simulation"
E_PREP_START_RDA = "prep_start_rda"
E_PREP_START_S_BAND = "prep_start_s_band"
E_PREP_START_UHF = "prep_start_uhf"
E_RDA_END = "rda_end"
E_RDA_START = "rda_start"
E_S_BAND_COM_END = "s_band_com_end"
E_S_BAND_COM_START = "s_band_com_start"
E_SHADOW_ENTER = "shadow_enter"
E_SHADOW_EXIT = "shadow_exit"
E_UHF_COM_END = "uhf_com_end"
E_UHF_COM_START = "uhf_com_start"


# RELEVANT COLUMNS
COL_DURATION = "Operation duration"
COL_SHADOW = "Shadow"
COL_MODE = "Operational Mode"
COL_TIME = "Time"
COL_EVENT = "Event"

# GLOBAL
ROW_IDX = 1


@dataclass
class MissionState:
    s_band_start: Optional[datetime] = None
    uhf_band_start: Optional[datetime] = None
    rda_start: Optional[datetime] = None

    stored_telemetry: float = 0.0
    sent_telemetry: float = 0.0
    stored_summaries: float = 0.0
    sent_summaries: float = 0.0
    wasted_uhf_band: float = 0.0

    total_images: float = 0.0
    stored_image_data: float = 0.0
    sent_image_data: float = 0.0
    wasted_s_band: float = 0.0


def mb(size):
    return round(size / (10**6), 1)


# def process_step(state: MissionState, step: pd.Series):
#     current_time = colored(f"{step[COL_TIME]}", "light_magenta")
#
#     if OP_SBAND in step[COL_MODE]:
#         maximum_transferable = SBAND_BPS * step[COL_DURATION].total_seconds()
#         needed = state.stored_image_data - state.sent_image_data
#
#         state.sent_image_data += min(needed, maximum_transferable)
#
#         transfer_status = ""
#         if needed == 0:
#             transfer_status = colored("NO", "red")
#         elif maximum_transferable > needed:
#             transfer_status = colored("COMPLETE", "green")
#         else:
#             transfer_status = colored("PARTIAL", "green")
#
#         unused = mb(max(maximum_transferable - needed, 0))
#         if unused == 0:
#             unused = colored(f"{unused}", "green")
#         elif needed > 0:
#             unused = colored(f"{unused}", "yellow")
#         else:
#             unused = colored(f"{unused}", "red")
#
#         transferable_colored = colored(f"{mb(maximum_transferable)}", "green")
#         needed_colored = ""
#
#         if needed == 0:
#             needed_colored = colored(f"{mb(needed)}", "dark_grey")
#         else:
#             needed_colored = colored(f"{mb(needed)}", "yellow")
#
#         op_mode = colored("SBAND", "cyan")
#         print(
#             f"{current_time}: {op_mode} {transfer_status} TRANSFER, {transferable_colored} Mb maximum transferable, {needed_colored} Mb needed, {unused} Mb wasted"
#         )
#
#     if (
#         OP_RDA in step[COL_EVENT]
#         and E_START in step[COL_EVENT]
#         and E_PREP not in step[COL_EVENT]
#     ):
#         previously_unsent = state.stored_image_data - state.sent_image_data
#
#         acquired_data = RDA_ACQUIRED_DATA / LOSSLESS_COMPRESSION_RATE
#         state.total_images += PHOTOS_DURING_RDA
#         state.stored_image_data += acquired_data
#
#         op_mode = colored("RDA", "cyan")
#         print(
#             f"{current_time}: {op_mode}   {mb(acquired_data)} Mb generated currently, {mb(state.stored_image_data)} Mb generated overall, {state.total_images} photos overall, {mb(previously_unsent)} Mb unsent from previous RDA"
#         )
#


def normalize(col: str):
    return re.sub(r"\s+", " ", col).strip().lower().replace(" ", "_")


def check_header(col, expected):
    if col != expected:
        raise ValueError(
            f"Unexpected header, was expecting '{expected}', but encountered '{col}'"
        )


def check_is_opened(value, event):
    global ROW_IDX

    if value is None:
        raise ValueError(
            f"Unexpected event, encountered '{event}' twice, without a corresponding start, at row {ROW_IDX}"
        )


def check_is_closed(value, event):
    global ROW_IDX

    if value is not None:
        raise ValueError(
            f"Unexpected event, encountered '{event}' twice, without a corresponding end, at row {ROW_IDX}"
        )


def normalize_time(time: str):
    global ROW_IDX

    if "/" in time:
        return datetime.strptime(time, "%d/%m/%Y %H:%M")
    else:
        try:
            return datetime.strptime(time, "%d-%b-%Y %H:%M:%S")
        except:
            pass

        try:
            return datetime.strptime(time, "%d-%B-%Y %H:%M:%S")
        except:
            raise ValueError(f"Unrecognized time '{time}', at row {ROW_IDX}")


def normalize_event(event: str):
    event = normalize(event)
    if event == E_START_SIMULATION:
        return event

    # get rid of "orbit x" or "  - x" suffixes
    return "_".join(event.split("_")[:-2]).replace("-", "_")


def normalize_duration(duration: str):
    split_duration = [int(x) for x in duration.split(":")]
    return timedelta(
        hours=split_duration[0], minutes=split_duration[1], seconds=split_duration[2]
    )


def process_row(state: MissionState, row):
    (
        time,
        orbit,
        duration,
        shadow,
        event,
        op_mode,
        op_desc,
        adcs_mode,
        comments,
        useful_info,
        _,
    ) = row

    time = normalize_time(time)
    duration = normalize_duration(duration)
    event = normalize_event(event)

    # TODO: fix this, this would assume that we are sending full frames, ebcause the constants are not updated
    state.stored_summaries += SUMMARY_FRAME_SIZE * (
        duration.total_seconds() / SUMMARY_COLLECTION_PERIOD_S
    )

    if event == E_START_SIMULATION:
        return

    # actually process the event
    if (
        event == E_PREP_START_RDA
        or event == E_PREP_START_S_BAND
        or event == E_PREP_START_UHF
    ):
        # we do not really do anything in the prep stages,
        # so just ignore all of them
        pass
    elif event == E_SHADOW_ENTER or event == E_SHADOW_EXIT:
        # shadow is only relevant for power generation, we do not
        # simulate that at the moment, so ignore it as well
        pass
    elif event == E_RDA_START:
        check_is_closed(state.rda_start, event)
        state.rda_start = time

    elif event == E_RDA_END:
        check_is_opened(state.rda_start, event)
        state.rda_start = None

        state.total_images += PHOTOS_DURING_RDA
        state.stored_image_data += RDA_ACQUIRED_DATA / LOSSY_COMPRESSION_RATE

    elif event == E_S_BAND_COM_START:
        check_is_closed(state.s_band_start, event)
        state.s_band_start = time

    elif event == E_S_BAND_COM_END:
        check_is_opened(state.s_band_start, event)
        delta: timedelta = time - state.s_band_start
        state.s_band_start = None

        potential_transfer = SBAND_BPS * delta.total_seconds()
        print(SBAND_BPS)
        print(delta.total_seconds())
        print(potential_transfer)
        needed = state.stored_image_data - state.sent_image_data

        state.wasted_s_band += max(potential_transfer - needed, 0)
        state.sent_image_data += min(needed, potential_transfer)

    elif event == E_UHF_COM_START:
        check_is_closed(state.uhf_band_start, event)
        state.uhf_band_start = time

    elif event == E_UHF_COM_END:
        check_is_opened(state.uhf_band_start, event)
        delta: timedelta = time - state.uhf_band_start
        state.uhf_band_start = None

        # TODO: telecommands + actual frame data
        potential_transfer = UHF_BPS * delta.total_seconds()
        needed = state.stored_summaries - state.sent_summaries

        state.wasted_uhf_band += max(potential_transfer - needed, 0)
        state.sent_summaries += min(needed, potential_transfer)

    else:
        raise ValueError(f"Unrecognized event: '{event}'")

    # Telemetry is always processed so, add it at the end of the processing
    global ROW_IDX
    ROW_IDX += 1


def print_state(state: MissionState):
    global ROW_IDX

    (
        time,
        orbit,
        duration,
        shadow,
        event,
        op_mode,
        op_desc,
        adcs_mode,
        comments,
        useful_ino,
        _,
    ) = row

    time = normalize_time(time)
    duration = normalize_duration(duration)
    event = normalize_event(event)

    def process_number_to_MiB(num: float):
        return num / 8 / 10**6

    # This looks like shit
    print(f"============== {time} : {event:20} =================")
    print(
        f"Stored summaries : {process_number_to_MiB(state.stored_summaries):20.6f} MiB"
    )
    print(f"Sent   summaries : {process_number_to_MiB(state.sent_summaries):20.6f} MiB")
    print(
        f"To be transfered : {process_number_to_MiB(state.stored_summaries - state.sent_summaries):20.1f} MiB"
    )
    print(
        f"Wasted UHF       : {process_number_to_MiB(state.wasted_uhf_band):20.6f} MiB"
    )
    print()
    print(
        f"Stored image data: {process_number_to_MiB(state.stored_image_data):20.6f} MiB"
    )
    print(
        f"Sent   image data: {process_number_to_MiB(state.sent_image_data):20.6f} MiB"
    )
    print(
        f"To be transfered : {process_number_to_MiB(state.stored_image_data - state.sent_image_data):20.6f} MiB"
    )
    print(f"Wasted S-Band    : {process_number_to_MiB(state.wasted_s_band):20.6f} MiB")
    print()
    # print(state.sent_summaries)
    # print(state.wasted_uhf_band)


if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument("-f", "--input-file", required=True)
    args = argparser.parse_args()

    with open(args.input_file, "r") as f:
        reader = csv.reader(f)

        try:
            header = next(reader)
        except:
            print(
                f"Malformed input file {args.input_file}, could not read header",
                file=stderr,
            )
            exit(1)

        header = [normalize(h) for h in header]
        (
            time_h,
            orbit_h,
            duration_h,
            shadow_h,
            event_h,
            op_mode_h,
            op_desc_h,
            adcs_mode_h,
            comments_h,
            useful_info_h,
            _,
        ) = header

        check_header(time_h, "time")
        check_header(orbit_h, "orbit_number")
        check_header(duration_h, "operation_duration")
        check_header(shadow_h, "shadow")
        check_header(event_h, "event")
        check_header(op_mode_h, "operational_mode")
        check_header(op_desc_h, "operation_description")
        check_header(adcs_mode_h, "adcs_mode")
        check_header(comments_h, "comments")
        check_header(useful_info_h, "useful_information")

        state = MissionState()
        for row in reader:
            process_row(state, row)
            print_state(state)
