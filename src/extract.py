import os
import csv
from pathlib import Path
import io
import pandas as pd
import numpy as np
import matplotlib as plt

from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
load_dotenv() 

df_base_line  = pd.read_csv("Data/01_baseline.csv")
print(df_base_line.head())
df_base_line  = pd.read_csv("Data/01_baseline.csv")
df_base_line.hist(bins=50, figsize=(20,15))
plt.show()


df_year_one= pd.read_csv("Data/02_year_one.csv")
print(df_year_one.head())
df_year_one.hist(bins=50, figsize=(20,15))
plt.show()


df_year_two = pd.read_csv("Data/03_year_two.csv")
print(df_year_two.head())
df_year_two.hist(bins=50, figsize=(20,15))
plt.show()



# Summary statistics
print(df_base_line.describe())
print(df_year_one.describe())
print(df_year_two.describe())

# Check for missing values
print(df_base_line.isnull().sum())
print(df_year_one.isnull().sum())
print(df_year_two.isnull().sum())

# Data types
print(df_base_line.dtypes)



# Append Solution to df_split_train
combined_data = pd.merge(df_base_line, df_year_one,df_year_two, on='hhid2', how='inner')
combined_data.info()
combined_data.describe()

#Show some visualisations for the data
combined_data.hist(bins=50, figsize=(20,15))
plt.show()


