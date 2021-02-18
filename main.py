import pandas as pd

from myWiki import WikiDecadeETL


def run(query:str="2010s"):

    decade = WikiDecadeETL(query)
    decade.load()
    return decade.core_df


def get_combined_df(start: int = 1900, stop: int = 2020) -> dict:

    lofdcm = WikiDecadeETL()
    drange_links = lofdcm.get_drange_links(start, stop)

    combined_dict = {query: run(query) for query in drange_links.keys()}

    combined_df = pd.concat(combined_dict.values(), ignore_index=True)

    return combined_df

def save_df(start, stop):

    combined_df = get_combined_df(start, stop)

    combined_df.to_csv(f"data/{start}_{stop}s.csv", index=False)


if __name__ == "__main__":
    print("Work In Progress")