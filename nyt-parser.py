#!/usr/bin/env python
# coding: utf-8

# In[112]:


from bs4 import BeautifulSoup
import requests
import re
import textract
import pandas as pd
import os


# In[2]:


link = "https://www.nytimes.com/interactive/2021/us/civilian-casualty-files.html"


# In[6]:


b = requests.get(link)


# In[9]:


doc = BeautifulSoup(b.text)


# In[120]:


def extract_pdf(pdf_path: str) -> str:
    if len(pdf_path) == 0:
        return ""
    return textract.process(pdf_path, encoding="utf-8").decode("utf-8")


# In[23]:


files = doc.find('div', attrs={'data-preview-slug': '2021-11-12-airstrikes-docs'})


# In[29]:


KINDS = ['credible-reports', 'noncredible-reports', 'process-docs']
reports = {}
for kind in KINDS:
    reports[kind] = files.find('div', attrs={'id': kind}).find('div', id=lambda x: x.endswith("-table"))


# In[82]:


ROW_TITLES = [
    'date',
    'location',
    'killed',
    'injured',
]

def parse_table(reports_table, row_titles=ROW_TITLES):
    rows = reports_table.findAll('div', recursive=False)
    data = []
    for r in rows[1:]:
        if not r:
            continue
        d = {}
        
        for title in row_titles:
            try:
                d[title] = r.find('div', class_=lambda x: x.endswith(title)).text.strip() 
            except Exception as e:
                print(f"couldn't find the column {title}")
        try:
            d['docs'] = r.find('div', class_=lambda x: x.endswith('docs'))
            if len(d['docs'].text.strip()) > 0:
                d['docs'] = d['docs'].find('a').attrs['href']
            else:
                d['docs'] = ''
            d['pr'] = r.find('div', class_=lambda x: x.endswith('pr'))
            if len(d['pr'].text.strip()) > 0:
                d['pr'] = d['pr'].find('a').attrs['href']
            else:
                d['pr'] = ''
        except Exception as e:
            print(f"didn't find {r} because of {str(e)}")
        data.append(d)
        
    return pd.DataFrame(data)


# In[113]:


def download_pdf(link):
    if len(link) == 0: 
        return ''
    out = '-'.join(link.split('/')[5:])
    out = f"./downloads/{out}"
    if os.path.exists(out):
        return out
    with open(out, "wb+") as outfile:
        r = requests.get(link)
        outfile.write(r.content)
    return out
        
def download_and_parse(df):
    df['doc_file'] = df['docs'].apply(download_pdf)
    df['pr_file'] = df['pr'].apply(download_pdf)

    return df


# In[131]:


def extract_pdf_from_pandas(df):
    df['doc_text'] = df['doc_file'].apply(extract_pdf)
    df['pr_text'] = df['pr_file'].apply(extract_pdf)
    return df


# In[139]:


def write_output(out_path, df):
    with open(out_path, "w+") as out:
        for row in df.itertuples():
            date = row.date
            loc = row.location
            try: 
                killed = row.killed
                injured = row.injured
            except:
                killed = "UNKNOWN"
                injured = "UNKNOWN"
                
            doc_text = row.doc_text
            pr_text = row.pr_text
            
            header_str = f"{date} - {loc}\nKilled: {killed}\nInjured: {injured}\n"
            out.write(header_str)
            out.write("DOCUMENT TEXT BELOW----\n")
            out.write(doc_text)
            out.write("PR TEXT BELOW----\n")
            out.write(pr_text)
            out.write("----------------------------------------------")


# In[2]:


def full_parse(kind, row_titles=ROW_TITLES):
    df = parse_table(reports[kind], row_titles)
    df = download_and_parse(df)
    df = extract_pdf_from_pandas(df)
    write_output(f"./output/{kind}.txt", df)


# In[1]:


full_parse("noncredible-reports", [
    'date',
    'location',
])


# In[ ]:




