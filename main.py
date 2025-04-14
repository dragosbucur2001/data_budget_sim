from dataclasses import dataclass
from termcolor import colored
import pandas as pd
import sys

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
OP_NOMINAL = "Nominal"
OP_NOMINAL_PREP = "Nominal Prep"
OP_SBAND = "S-band Communication"
OP_RDA = "RDA"
OP_UFG = "UHF"
OP_IA = "IA"

# EVENT NAMES
E_START = "Start"
E_END = "End"
E_PREP = "Prep"


# RELEVANT COLUMNS
COL_DURATION = "Operation duration"
COL_SHADOW = "Shadow"
COL_MODE = "Operational Mode"
COL_TIME = "Time"
COL_EVENT = "Event"


@dataclass
class MissionState:
    stored_telemetry: float = 0
    sent_telemetry: float = 0

    stored_summaries: float = 0
    sent_summaries: float = 0

    total_images: float = 0
    stored_image_data: float = 0
    sent_image_data: float = 0


def mb(size):
    return round(size / (10**6), 1)


def process_step(state: MissionState, step: pd.Series):
    current_time = colored(f"{step[COL_TIME]}", "light_magenta")

    if OP_SBAND in step[COL_MODE]:
        maximum_transferable = SBAND_BPS * step[COL_DURATION].total_seconds()
        needed = state.stored_image_data - state.sent_image_data

        state.sent_image_data += min(needed, maximum_transferable)

        transfer_status = ""
        if needed == 0:
            transfer_status = colored("NO", "red")
        elif maximum_transferable > needed:
            transfer_status = colored("COMPLETE", "green")
        else:
            transfer_status = colored("PARTIAL", "green")

        unused = mb(max(maximum_transferable - needed, 0))
        if unused == 0:
            unused = colored(f"{unused}", "green")
        elif needed > 0:
            unused = colored(f"{unused}", "yellow")
        else:
            unused = colored(f"{unused}", "red")

        transferable_colored = colored(f"{mb(maximum_transferable)}", "green")
        needed_colored = ""

        if needed == 0:
            needed_colored = colored(f"{mb(needed)}", "dark_grey")
        else:
            needed_colored = colored(f"{mb(needed)}", "yellow")

        op_mode = colored("SBAND", "cyan")
        print(
            f"{current_time}: {op_mode} {transfer_status} TRANSFER, {transferable_colored} Mb maximum transferable, {needed_colored} Mb needed, {unused} Mb wasted"
        )

    if (
        OP_RDA in step[COL_EVENT]
        and E_START in step[COL_EVENT]
        and E_PREP not in step[COL_EVENT]
    ):
        previously_unsent = state.stored_image_data - state.sent_image_data

        acquired_data = RDA_ACQUIRED_DATA / LOSSLESS_COMPRESSION_RATE
        state.total_images += PHOTOS_DURING_RDA
        state.stored_image_data += acquired_data

        op_mode = colored("RDA", "cyan")
        print(
            f"{current_time}: {op_mode}   {mb(acquired_data)} Mb generated currently, {mb(state.stored_image_data)} Mb generated overall, {state.total_images} photos overall, {mb(previously_unsent)} Mb unsent from previous RDA"
        )


if __name__ == "__main__":
    data = pd.read_csv("data.csv")
    if type(data) != pd.DataFrame:
        print("Could not load data file")
        sys.exit(-1)

    data = data.rename(columns=lambda x: x.strip())
    data[COL_DURATION] = pd.to_timedelta(data[COL_DURATION])
    data[COL_TIME] = pd.to_datetime(data[COL_TIME])

    state = MissionState()
    for i, row in data.iterrows():
        if i > 300:
            break

        process_step(state, row)
