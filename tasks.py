import pandas as pd

from etl import WikiDecadeETL


def run(query:str)->WikiDecadeETL:
    
    decade = WikiDecadeETL(query)
    decade.load()
    return decade


def get_df(start:int=1900, stop:int=2020)->pd.DataFrame:
    
    lofdcm = WikiDecadeETL()
    drange_links = lofdcm.get_drange_links(start, stop)
    
    combined_sects = {query: run(query).core_df for query in drange_links.keys()}
    
    combinedSects_df = pd.concat(combined_sects.values(), ignore_index=True)

    combined_summ = {query: run(
        query).page.summary for query in drange_links.keys()}

    combinedSumm_df = pd.DataFrame.from_dict(combined_summ, orient="index", columns=["summary"])\
        .reset_index()\
        .rename(columns={"index": "decade"})

    full_df = pd.merge(combinedSects_df, combinedSumm_df,
                       how="inner", on="decade")
    
    return full_df

# def get_combinedSumm_df(start:int=1900, stop:int=2020)->dict:
    
#     lofdcm = WikiDecadeETL()
#     drange_links = lofdcm.get_drange_links(start, stop)
    
#     combined_dict = {query: run(query).page.summary for query in drange_links.keys()}
    
#     combined_df = pd.DataFrame.from_dict(tst, orient="index", columns=["summary"])\
#     .reset_index()\
#     .rename(columns={"index": "decade"})
    
#     return combined_df
        

def save_df(start:int=1900, stop:int=2020, version=2, _func=None, annot=''):

    combined_df = _func(start, stop)

    combined_df.to_csv(f"data/v{version}_{start}_{stop}s_{annot}.csv", index=False)


if __name__ == "__main__":
    print("Work In Progress")
