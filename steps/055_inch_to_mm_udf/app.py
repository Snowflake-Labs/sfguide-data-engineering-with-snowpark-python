# ------------------------------------------------------------------------------
# Hands-On Lab: Data Engineering with Snowpark
# Script:       055_inch_to_mm_udf/app.py
# Author:       Atzmon
# Last Updated: 5/5/2023
# ------------------------------------------------------------------------------

# SNOWFLAKE ADVANTAGE: Snowpark Python programmability
# SNOWFLAKE ADVANTAGE: Python UDFs (with third-party packages)
# SNOWFLAKE ADVANTAGE: SnowCLI (PuPr)

import sys


def main(temp_f: float) -> float:
    return float(temp_f) * 25.4


# For local debugging
# Be aware you may need to type-convert arguments if you add input parameters
if __name__ == "__main__":
    if len(sys.argv) > 1:
        print(main(*sys.argv[1:]))  # type: ignore
    else:
        print(main())  # type: ignore
