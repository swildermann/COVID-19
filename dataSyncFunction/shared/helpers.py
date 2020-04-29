import pandas as pd

country_col = 'Country/Region'
province_col = 'Province/State'
district_col = 'District'
date_col = 'date'
string_cols = [country_col, province_col, district_col]


def cleanup_df(df_in, key_cols):
    df = df_in.copy()
    # replace nan with 0 in numeric cols
    fillna_dict = {'infections': 0, 'deaths': 0, 'recovered': 0}
    df = df.fillna(fillna_dict)
    # replace "None" with pd.NA
    for str_col in string_cols:
        if str_col in df and df[str_col].notnull().sum() > 0:
            df.loc[df[str_col].str.lower() == "none", str_col] = pd.NA

    # If Province == Recoverd  -> Province = pd.NA
    df.loc[df[province_col] == "Recovered", province_col] = pd.NA
    # replace certain countries based on province string match
    if df[province_col].notnull().sum() > 0:
        province_country_replace_dict = {'Hong Kong': 'China', 'Macau': 'China', 'Taiwan': 'Taiwan', 'Grand Princess': 'Cruise Ship',
                                         'Diamond Princess': 'Cruise Ship'
                                         }
        for key, value in province_country_replace_dict.items():
            # print(key, value)
            df.loc[df[province_col].fillna(
                '').str.contains(key), country_col] = value

    string_col_replacement = {key: "None" for key in string_cols}
    df = df.fillna(string_col_replacement)

    # replace certain countries based on country string match
    if df[country_col].notnull().sum() > 0:
        country_country_replace_dict = {'Gambia': 'Gambia', 'Congo': 'Congo', 'China': 'China', 'Czech': 'Czechia',
                                        'Dominica': 'Dominican Republic', 'UK': 'United Kingdom', 'Bahamas': 'Bahamas',
                                        'US': 'United States', 'Korea, South': 'South Korea', 'Taiwan': 'Taiwan',
                                        'Iran': 'Iran', 'Viet nam': 'Vietnam', 'Russia': 'Russia', 'Republic of Korea': 'South Korea',
                                        'Diamond Princess': 'Cruise Ship', 'Grand Princess': 'Cruise Ship'
                                        }
        # This command should work, but somehow it does not
        # df.loc[df[country_col].str.lower().str.contains(key), country_col] = value
        # --> Stack Overflow
        for key, value in country_country_replace_dict.items():
            if key in ['US', 'UK']:
                df.loc[df[country_col] == key, country_col] = value
            else:
                df.loc[df[country_col].str.contains(
                    key, case=False), country_col] = value

    agg_dict = {'FIPS': 'max', 'Lat': 'max', 'Long': 'max',
                'infections': 'sum', 'deaths': 'sum', 'recovered': 'sum'}
    to_remove = []
    for key in agg_dict:
        if key not in df.columns:
            to_remove.append(key)
    for rem_key in to_remove:
        agg_dict.pop(rem_key)

    df = df.groupby(by=key_cols).agg(agg_dict).reset_index()
    for col in string_cols:
        if col in df.columns:
            df.loc[df[col].str.contains("None"), col] = pd.NA

    return df


def translate_county(federalState):
    if federalState == "Bayern":
        federalState = "Bavaria"
    elif federalState == "Hessen":
        federalState = "Hesse"
    elif federalState == "Niedersachsen":
        federalState = "Lower Saxony"
    elif federalState == "Mecklenburg-Vorpommern":
        federalState = "Mecklenburg-Western Pomerania"
    elif federalState == "Nordrhein-Westfalen":
        federalState = "North Rhine-Westphalia"
    elif federalState == "Rheinland-Pfalz":
        federalState = "Rhineland-Palatinate"
    elif federalState == "Sachsen":
        federalState = "Saxony"
    elif federalState == "Sachsen-Anhalt":
        federalState = "Saxony-Anhalt"
    elif federalState == "Thüringen":
        federalState = "Thuringia"
    return federalState