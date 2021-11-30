import pandas as pd


def get_code_ens(text):
    if isinstance(text, str):
        if text.find("-") > 0:
            pos = text.find("-")
        elif text.find("_") > 0:
            pos = text.find("_")
        elif text.find("/"):
            pos = text.find("-")
        else:
            pos = -1
    else:
        pos = -1

    if pos > 0:
        return text[pos+1:].strip()
    else:
        return None


def get_data(file):
    columns = ['building_CodeEns_GPG', 'improvement_type_level1',
       'improvement_type_level2', 'improvement_type_level3',
       'improvement_type_level4', 'description', 'improvement_percentage',
       'date_start', 'Data de finalització de l obra / millora',
       'investment_without_tax', 'energy_type', 'observations']
    df = pd.read_excel(file, names=columns)
    df["codeEns"] = df["building_CodeEns_GPG"].apply(get_code_ens)
    return df.to_dict(orient="records")

