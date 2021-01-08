import pandas as pd

DATA_PATH = '.\data'
OUTPUT_PATH = '.\output'
JOB_ID_COL = 'Job-ID'
ACT_SHIP_DATE_COL = 'Act Ship-Date'
REQ_SHIP_DATE_COL = 'Req Ship-Date'

def get_input_path(filename):
    return DATA_PATH + '\\' + filename

def get_output_path(filename):
    return OUTPUT_PATH + '\\' + filename

def extract(filename):
    print(f'Extracting from filename {filename}')
    df = pd.read_excel(get_input_path(filename), sheet_name=0)
    print(df.head())
    df = df[[JOB_ID_COL, ACT_SHIP_DATE_COL]]
    df = df.groupby([JOB_ID_COL])
    print(df.head())
    return df

def convert_date(date):
    if date.month > 7:
        job_ym = f'{date.year+1}-{date.month-7:02}'
    else:
        job_ym = f'{date.year}-{date.month+5:02}'

def process_gb(df, res):
    job_id = df[JOB_ID_COL].iloc[0]
    job_class = 0
    job_desc = None
    job_ym = None
    #print(job_id)
    if df.shape[0] == 1:
        date = df[ACT_SHIP_DATE_COL].iloc[0]
        if pd.isnull(date):
            job_class = 4
            job_desc = 'No Date'
        else:
            job_class = 0
            job_desc = date
            job_ym = convert_date(date)
    elif df.shape[0] > 1:
        dates = df[ACT_SHIP_DATE_COL]
        dates = dates.sort_values()
        if dates.isnull().all():
            job_class = 4
            job_desc = 'No Date - multiple entries'
        else:
            min_dt = dates.min()
            max_dt = dates.max()
            job_class = 1
            job_desc = min_dt
            job_ym = convert_date(min_dt)
            if min_dt.year != max_dt.year or min_dt.month != max_dt.month:
                job_class = 2
                if (max_dt - min_dt).days > 10:
                    job_class = 3
                    job_desc = 'Pending investigation'
                    job_ym = None
    tmp = [job_id, job_ym, None, None, None, None, None]
    tmp[job_class + 2] = job_desc
    res.append(tmp)


def process(dfgb):
    res = []
    dfgb.apply(process_gb, res)
    return res

def create_df(data):
    columns = ['Job ID', 'YYYY-MM', 'A - Single Valid Entry', 'B - Same month multiple entries', 'C - Different month, gap <= 10 days', 'D - Different month, gap > 10 days', 'E - No date']
    df = {c:list() for c in columns}
    for row in data:
        for i in range(len(columns)):
            df[columns[i]].append(row[i])
    return pd.DataFrame(df)

def main():
    filename = '2018-2020.xls'
    data = extract(filename)
    res = process(data)
    df = create_df(res)
    print(df.head())
    df.to_excel(get_output_path(filename), index=False)

if __name__ == "__main__":
    main()
