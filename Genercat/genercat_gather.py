import pandas as pd


def get_data(file):
    columns = ['building_CodeEns_GPG', 'improvement_type_level1',
       'improvement_type_level2', 'improvement_type_level3',
       'improvement_type_level4', 'description', 'improvement_percentage',
       'date_start', 'Data de finalització de l obra / millora',
       'investment_without_tax', 'energy_type', 'observations']
    df = pd.read_excel(file, names=columns)
    return df.to_dict(orient="records")

