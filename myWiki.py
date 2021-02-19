import pandas as pd
from typing import List, Dict, Tuple
from abc import ABC, abstractmethod


import wikipediaapi as wk

class AbstractETL(ABC):
    
    @abstractmethod
    def extract():
        pass

    @abstractmethod
    def transform():
        pass

    @abstractmethod
    def load():
        pass

class WikiDecadeETL(AbstractETL):
    """
    Getting data specifically from WikiPedia.
    In this case wiki pages on historical events data by decade
    """
    ignore_sections = {'Pronunciation varieties','Name for the decade','Further reading','References','External links', "Notes"}
    
    root_query = "List of decades, centuries, and millennia"
    
    def __init__(self, query:str=root_query)->None:
        self.query = query
        self._service = wk.Wikipedia("en", extract_format=wk.ExtractFormat.WIKI)
        self.page = self._service.page(query)
        self.core_sections = {};
        self.coreSect_sub = {}
        self.core_df = None
        
    def get_drange_links(self, start:int, stop:int) -> dict:
        
        """
        # TODO
        
        This function filters the decade range of interest and is specific to 
        the root_query
        """
        
        drange = [f"{str(i)}s" if str(i)[-2:] != "00" else f"{str(i)}s (decade)" for i in range(start, stop+1, 10)]
        
        drange_links = dict(zip(drange, map(self.page.links.get, drange)))
        
        return drange_links
        
    
    def get_page_sections(self)->Tuple[list, dict]:
        """
        Returns dict of wiki page sections, subsections and text
        """
        main_sections = self.page.sections # We have to get the section before getting the section mapping
        all_sections_dict = self.page._section_mapping # section mapping is empty if above is not executed first

        return main_sections, all_sections_dict

    
    def core_section_extractor(self)->None:
        
        """
        Params: decade
        Returns dict of sections_title of key interest that will later be used to extract a sections content
            main_section_title : list of subsections
        """
        
        main_sections, all_sections_dict = self.get_page_sections()
        
        # All sections. Main, Subsections and Sections to ignore
        all_section_titles = list(all_sections_dict.keys())

        # Only the core sections including "See also"
        core_section_titles = [s.title for s in main_sections if s.title not in self.ignore_sections]
        

        # Storing the core section indices according to their position in the all_section_title list
        core_indices = {k: all_section_titles.index(k) for k in  core_section_titles}

        # Convinience variable 
        indices_lst = list(core_indices.keys())


        # Store the core title with a list of its subsections
        # core_indices = {"People": 14, "See Also": 16} - # People is on index 14 on all_section_index with possible 2 subsections
        # index_lst = ["People", "See Also"] - People is at index 0 of core_indices.keys()
        # core_dict = {"People":["World Leaders", "Business Leaders"]}
        self.coreSect_sub = {indices_lst[i]:all_section_titles[core_indices[indices_lst[i]]+1: core_indices[indices_lst[i+1]]] 
                                   for i in range(len(indices_lst)-1)}
        
        sect_titles = self.coreSect_sub.keys()
        # Subseting the all_sections_dict to only the core_sections with subs embeded
        self.core_sections = dict(zip(sect_titles, map(all_sections_dict.get, sect_titles)))
        
        
    def get_subtitle(self, val):
        
        """
        Returns the subsections title list for a section in an entry.
        
        If subsections do not exist return the section title
        """
        
        res = val
        if self.coreSect_sub[val]:
            res = self.coreSect_sub[val]
        return res
    
    def get_subtext(self, val):
        """
        Returns the a subsections full texts for an entry
        """
        return self.page.section_by_title(val).full_text()
    
    def get_df(self):
        
        """
        Creating a dataframe from extracted data
        """
        
        core_sections = self.core_sections
        
        temp_df = pd.DataFrame.from_dict(core_sections, orient="index", 
                                         columns=["text"]).reset_index().rename(columns={"index": "section"})
        
        temp_df["sub_section"] = temp_df["section"].apply(self.get_subtitle)
        
        temp_df = temp_df.explode("sub_section", ignore_index=True)
        
        temp_df["text"] = temp_df["sub_section"].apply(self.get_subtext)
        
        temp_df["decade"] = self.query
        
        self.core_df = temp_df
        
    def extract(self):
        # Mainly for fetching the data we want from Wikipedia
        self.core_section_extractor()
    
    def transform(self):
        # Processing the raw data retaining only the parts we want
        self.extract()
        
        self.get_df()
    
    def load(self):
        # Loading the semi-processed data in data frame format.
        self.transform()