#!/usr/bin/env python3

from dataclasses import dataclass
import csv
import argparse
from datetime import date, datetime, timedelta
from typing import Optional
from sys import stderr
import re

# === CONSTANTS ===

# IMAGE CONSTANTS
PHOTOS_DURING_RDA = 8
BITS_PER_PIXEL = 10
IMAGE_SIZE = 2048 * 2048
RDA_ACQUIRED_DATA = PHOTOS_DURING_RDA * BITS_PER_PIXEL * IMAGE_SIZE
LOSSY_COMPRESSION_RATE = 9
LOSSLESS_COMPRESSION_RATE = 1.7

# TELEMETRY CONSTANTS
SUMMARY_FRAME_SIZE = 1400
SUMMARY_COLLECTION_PERIOD_S = 10
DETAILED_FRAME_SIZE = 1400
DETAILED_COLLECTION_PERIOD_S = 10

# TRANSMISSIONS CONSTANTS
ENDURO_PROT_OVERHEAD = 1.7
FEC_RATE = 7 / 8
COMM_OVERHEAD = ENDURO_PROT_OVERHEAD * (1 / FEC_RATE)
SBAND_BPS_BASE = 100 * 10**3
UHF_BPS_BASE = 9600
SBAND_BPS = SBAND_BPS_BASE / COMM_OVERHEAD
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
    stored_telemetry: float = 0
    sent_telemetry: float = 0

    s_band_start: Optional[datetime] = None
    uhf_band_start: Optional[datetime] = None
    rda_start: Optional[datetime] = None

    stored_summaries: float = 0
    sent_summaries: float = 0

    total_images: float = 0
    stored_image_data: float = 0
    sent_image_data: float = 0


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


def check_open_close(value, event):
    global ROW_IDX

    if "start" in event:
        if value is not None:
            raise ValueError(
                f"Unexpected event, encountered '{event}' twice, without a corresponding end, at row {ROW_IDX}"
            )
    elif "end" in event:
        if value is None:
            raise ValueError(
                f"Unexpected event, encountered '{event}' twice, without a corresponding start, at row {ROW_IDX}"
            )
    else:
        raise ValueError(f"Unexpected event, encountered '{event}', at row {ROW_IDX}")


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

    # normalize some of the columns
    if "/" in time:
        time = datetime.strptime(time, "%d/%m/%Y %H:%M")
    else:
        try:
            time = datetime.strptime(time, "%d-%b-%Y %H:%M:%S")
        except:
            time = datetime.strptime(time, "%d-%B-%Y %H:%M:%S")

    duration = [int(x) for x in duration.split(":")]
    duration = timedelta(hours=duration[0], minutes=duration[1], seconds=duration[2])

    event = normalize(event)
    if event == E_START_SIMULATION:
        return

    # get rid of "orbit x" or "  - x" suffixes
    event = "_".join(event.split("_")[:-2]).replace("-", "_")

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
        check_open_close(state.rda_start, event)
        state.rda_start = time
    elif event == E_RDA_END:
        check_open_close(state.rda_start, event)
        state.rda_start = None
    elif event == E_S_BAND_COM_START:
        check_open_close(state.s_band_start, event)
        state.s_band_start = time
    elif event == E_S_BAND_COM_END:
        check_open_close(state.s_band_start, event)
        print("S-band", time - state.s_band_start)
        state.s_band_start = None
    elif event == E_UHF_COM_START:
        check_open_close(state.uhf_band_start, event)
        state.uhf_band_start = time
    elif event == E_UHF_COM_END:
        check_open_close(state.uhf_band_start, event)
        print("UHF", time - state.uhf_band_start)
        state.uhf_band_start = None
    else:
        raise ValueError(f"Unrecognized event: '{event}'")

    # Telemetry is always processed so, add it at the end of the processing
    global ROW_IDX
    ROW_IDX += 1


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
