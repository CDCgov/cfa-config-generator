# CFA Config Generator

## Overview

Scripts and utils to generate configuration objects for modeling pipelines. The first version of this will support [{EpiNow2}](https://github.com/epiforecasts/EpiNow2), but will be extensible to other
projects down the line.

## Project Admin

- Nathan McIntosh, UTE2@cdc.gov
- Micah Wiesner, ZQM6@cdc.gov
- Zack Susswein, UTB2@cdc.gov

## Use as a library
This repository can be used as a library for other projects. Below are two examples of using the functionality to generate config files in Blob.

```python
from datetime import date, datetime, timedelta

from cfa_config_generator.utils.epinow2.driver_functions import (
    generate_config,
    generate_rerun_config,
)
from cfa_config_generator.utils.epinow2.functions import generate_default_job_id

today: date = date.today()
now: datetime = datetime.now()

# Generate and upload to blob for all states and diseases.
generate_config(
    state="all",
    disease="all",
    report_date=today,
    reference_dates=[today - timedelta(days=1), today - timedelta(weeks=8)],
    data_path=f"gold/{today.isoformat()}.parquet",
    data_container="nssp-etl",
    production_date=today,
    job_id=generate_default_job_id(now.isoformat()),
    as_of_date=now.isoformat(),
    output_container="nssp-rt-testing",
)

# For reruns. Note that if we did not pass in state="all" and disease="all",
# there is a risk of accidentally excluding a state-disease pair that is in the
# data exclusions file. For example, if I ran this with state="NY" and
# disease="COVID-19", and the data exclusions file contained a row for
# "WA,Influenza", then no config would be generated for WA,Influenza.
# We should clean this up in the future.
generate_rerun_config(
    state="all",
    disease="all",
    report_date=today,
    reference_dates=[today - timedelta(days=1), today - timedelta(weeks=8)],
    data_path=f"gold/{today.isoformat()}.parquet",
    data_container="nssp-etl",
    production_date=today,
    job_id=generate_default_job_id(now.isoformat()),
    as_of_date=now.isoformat(),
    output_container="nssp-rt-testing",
    data_exclusions_path="az://nssp-etl/outliers-v2/2025-04-23.csv",
)
```

## General Disclaimer
This repository was created for use by CDC programs to collaborate on public health related projects in support of the [CDC mission](https://www.cdc.gov/about/organization/mission.htm).  GitHub is not hosted by the CDC, but is a third party website used by CDC and its partners to share information and collaborate on software. CDC use of GitHub does not imply an endorsement of any one particular service, product, or enterprise.

## Public Domain Standard Notice
This repository constitutes a work of the United States Government and is not
subject to domestic copyright protection under 17 USC § 105. This repository is in
the public domain within the United States, and copyright and related rights in
the work worldwide are waived through the [CC0 1.0 Universal public domain dedication](https://creativecommons.org/publicdomain/zero/1.0/).
All contributions to this repository will be released under the CC0 dedication. By
submitting a pull request you are agreeing to comply with this waiver of
copyright interest.

## License Standard Notice
This repository is licensed under ASL v2 or later.

This source code in this repository is free: you can redistribute it and/or modify it under
the terms of the Apache Software License version 2, or (at your option) any
later version.

This source code in this repository is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the Apache Software License for more details.

You should have received a copy of the Apache Software License along with this
program. If not, see http://www.apache.org/licenses/LICENSE-2.0.html

The source code forked from other open source projects will inherit its license.

## Privacy Standard Notice
This repository contains only non-sensitive, publicly available data and
information. All material and community participation is covered by the
[Disclaimer](https://github.com/CDCgov/template/blob/master/DISCLAIMER.md)
and [Code of Conduct](https://github.com/CDCgov/template/blob/master/code-of-conduct.md).
For more information about CDC's privacy policy, please visit [http://www.cdc.gov/other/privacy.html](https://www.cdc.gov/other/privacy.html).

## Contributing Standard Notice
Anyone is encouraged to contribute to the repository by [forking](https://help.github.com/articles/fork-a-repo)
and submitting a pull request. (If you are new to GitHub, you might start with a
[basic tutorial](https://help.github.com/articles/set-up-git).) By contributing
to this project, you grant a world-wide, royalty-free, perpetual, irrevocable,
non-exclusive, transferable license to all users under the terms of the
[Apache Software License v2](http://www.apache.org/licenses/LICENSE-2.0.html) or
later.

All comments, messages, pull requests, and other submissions received through
CDC including this GitHub page may be subject to applicable federal law, including but not limited to the Federal Records Act, and may be archived. Learn more at [http://www.cdc.gov/other/privacy.html](http://www.cdc.gov/other/privacy.html).

## Records Management Standard Notice
This repository is not a source of government records but is a copy to increase
collaboration and collaborative potential. All government records will be
published through the [CDC web site](http://www.cdc.gov).
