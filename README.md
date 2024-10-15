# CDAWGG:

## Data processor code - processing CSV files into JSON

![v2 cover image cdawgg](https://github.com/user-attachments/assets/169b2b21-db46-419a-81e5-c72839a3520b)

[`CDAWGG Demo Video`](https://drive.google.com/file/d/1scRXFMOxK1mjKi938ZdaijKhMRynr_5P/view?usp=sharing)

[`Phase 2 Improvements`](https://cdawgg.oofdev.com/embed_phase2_cdawgg_trimmed.mp4)


### Deployed link : [`Live Site`](https://cdawgg.oofdev.com/)



## Getting Started
Needs Python to be installed in your system. (conda or mini-conda env recommened).
Please run `pip install -r requirements.txt` to install all project required dependencies.
```bash

pip install -r requirements.txt

```
- After installation - run the `1_combine_data_files.py` first.
- Once it completes , run `2_process_data_files.py` .This would generate JSON files for CDAWGG.
- Reviews are pre-processed using gemini api using Review Processor module within CDAWGG.
(this was done to save time & keep writing code in JavaScript - which greatly did help out.)


