import pandas as pd
import unicodedata
import re


def normalize_column_name(col):
    
    col = col.strip()                 # enlever espaces début/fin
    col = col.lower()                 # minuscules
    
    # enlever accents
    col = unicodedata.normalize('NFKD', col)
    col = col.encode('ascii', 'ignore').decode('utf-8')
    
    # remplacer espaces et tirets
    col = col.replace(" ", "_")
    col = col.replace("-", "_")
    
    # enlever caractères spéciaux
    col = re.sub(r'[^a-z0-9_]', '', col)
    
    return col


def clean_columns(df):
    
    df.columns = [normalize_column_name(c) for c in df.columns]
    return df


def remove_duplicates(df):
    
    return df.drop_duplicates()


def standardize_text(series):
    
    return series.astype(str).str.strip().str.lower()


def split_clean_rejects(df, mask):
    
    rejects = df[mask]
    clean = df[~mask]
    
    return clean, rejects