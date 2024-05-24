import itertools
import json
import re
from typing import Dict, List, Optional, Tuple

from . import ELE_ANUM


## Hashdict
class Hashdict(dict):
    """
    Designed for enabling dictionary matching (as keys of a mapping dict)
    Internally the keys are int type (atomic number) 
    and values are a list of two floats (lower and upper)
    
    Inheritance from dict class
    """
    def __hash__(self):
        return hash(frozenset(self.items()))


## Main Class for chem_form whacking
class ChemFormNormalizer():
    """
    Class for providing normalisation of Substance NER in the form of chemical formulae
    Converting it to a hashable dict (defined outside of this class)
    To be mapped to substance_id via mapping to a dict with hashable dicts as keys
    
    Note that Deuterium and Tritium ("D" and "T") were dropped for this first version
    """
    #### Settings ####
    def __init__(self, ele_anum_path: str=ELE_ANUM):
        ## regex and transtable inits
        self.float_regex = re.compile(r"[\d]+(\.\d+)?")
        self.subs_trans = str.maketrans("₀₁₂₃₄₅₆₇₈₉——═", "0123456789--=")
        self.cleaning_regex = re.compile(r"((<\/?sub>)(?=\d))|((?<=\d)<\/?sub>)|(\(I+\))")
        self.discard_regex = re.compile(r"[/><,≡=×]")
        self.bracket_regex = re.compile(r"\(([A-Za-z]+)\)(\d+)")
        
        ## filtering sets
        self.start_rejects = (":", ')')
        self.end_rejects = (':', '(', 'Ac')
        
        ## loading of the ele_anum dictionary
        self.ele_anum_path = ele_anum_path
        self.ele_anum = self._load_json(ele_anum_path)
        
        ## version
        self.version = 0.1
        
    def __str__(self):
        return f"<Chemical formula normalizer, version={self.version}, ele_anum_path={self.ele_anum_path}>"
        
    def __repr__(self):
        return f"<Chemical formula normalizer, version={self.version}, ele_anum_path={self.ele_anum_path}>"
        
    #### Main Function ####
    def extract_substance_hashdict(
        self,
        raw_comp: str,
    ) -> Optional[Hashdict]:
        """
        Main function
        Takes in raw Substance NER and returns a hashdict for comparison
        """
        # Extraction from NER
        composition_tuplelist = self.breakdown_to_compounds(raw_comp)
        if len(composition_tuplelist) == 0:
            return None

        # Compilation into dictionary
        composition_dict = self.collapse_dict(composition_tuplelist)

        # element check and convertion to hashdict
        substance_hashdict = self.hash_compdict(composition_dict)

        return substance_hashdict
    
    #### Sub Functions ####
    def breakdown_to_compounds(self,component: str) -> List[Tuple[str, float, float]]:
        """
        Converts a string representation of a component inside the molecular formula
        into a list of compound strings and their multipliers

        :returns: List of (compound, lower_value, upper_value)
        """
        compound_list = []
        lower_list = []
        upper_list = []

        # splitting into alpha-strings and floats
        split_comp_list = self._split_expression(component)

        if split_comp_list is None:
            return []

        # init defaults
        comp_multiplier = 1.0  # acts on all subscript numbers
        compound_holder = ""
        default_value = 1.0

        for i, split_comp in enumerate(split_comp_list):
            # helper function to determine if float values exist
            is_float, lower, upper = self._handle_floats(split_comp)

            # case: float-type, multiplier (at the front)
            if (i == 0) and is_float:
                # case: single value
                if lower == upper:
                    comp_multiplier = lower
                # case: fractionals, "lower" first due to ordering of the numbers
                else:
                    if upper != 0:
                        comp_multiplier = lower / upper
                    else:
                        comp_multiplier = 0
            # case: float-type, subscript for prior component
            elif is_float:
                compound_list.append(compound_holder)
                lower_list.append(lower)
                upper_list.append(upper)
                # renew the substance
                compound_holder = ""
            # case: alpha-type, no prior substance
            elif compound_holder == "":
                compound_holder = split_comp
            # case: alpha-type, prior substance present
            else:
                compound_list.append(compound_holder)
                lower_list.append(default_value)
                upper_list.append(default_value)
                compound_holder = split_comp

        # handling last substance without subscript
        if compound_holder != "":
            compound_list.append(compound_holder)
            lower_list.append(default_value)
            upper_list.append(default_value)

        # multiplying the multiplier across all lower and upper values
        lower_list = [comp_multiplier * x for x in lower_list]
        upper_list = [comp_multiplier * x for x in upper_list]

        return [x for x in zip(compound_list, lower_list, upper_list)]
    
    def collapse_dict(
        self,
        composition_tuplelist: List[Tuple[str, float, float]]
    ) -> Dict[str,List[float]]:
        """
        Further splits each component into consituent elements (failsafe)
        Copies the float numerator for each related element
        Returns a tabulated dictionary with lower and upper values
        """
        element_dict = {}
        for tup in composition_tuplelist:
            compound, lower, upper = tup
            elements = self._compound_to_elements(compound)
            for e in elements:
                if not element_dict.get(e):
                    element_dict[e] = [lower,upper]
                else:
                    element_dict[e][0] += lower
                    element_dict[e][1] += upper

        return element_dict
    
    def hash_compdict(self, element_dict: Dict[str,List[float]]) -> Optional[Hashdict]:
        '''
        Checks if the "elements" are correct; returns None if elements are not correct
        Converts them into their atomic number and returns the Hashdict form
        '''
        prep_dict = {}
        for k, v in element_dict.items():
            if self.ele_anum.get(k):
                new_v = (f"{v[0]:.2f}", f"{v[1]:.2f}")
                prep_dict[str(self.ele_anum[k])] = new_v
            else:
                return None

        return Hashdict(prep_dict)
    
    #### Internal Helper Functions ####
    
    def _load_json(self, json_pathway: str) -> dict:
        with open(json_pathway, 'r') as f:
            return json.load(f)
    
    def _handle_floats(self, text_expr: str) -> Tuple[bool, float, float]:
        """
        Handles splitted component text to check if float values are present
        If present, float values (converted to range) are parsed and returned

        :returns: Boolean if text_expr has float-types, and the lower and upper values (0.0 in False case)
        """
        # init default lower and upper values
        lower, upper = 0.0, 0.0

        # checking for fractionals
        if "/" in text_expr:
            split_text = text_expr.split("/")

        # split by the range symbol "-"
        else:
            split_text = text_expr.split("-")

        # check if conversion to float is possible
        for i, subtext in enumerate(split_text):
            try:
                number = float(subtext)
                if i == 0:
                    lower = number
                    upper = number  # case: only one value, populate upper with lower
                else:
                    upper = number  # overwrite lower value

            # case: any of the parts cannot be converted to float
            except ValueError:
                return False, lower, upper

        # case: for-loop concluded without exception, float values obtained
        return True, lower, upper

    def _compound_to_elements(self, compound: str) -> List[str]:
        """
        Splits a compound into its constituent elements
        By relying on fact that all elements start with capital letter
        """
        element_list = []
        element_holder = ""
        for char in compound:
            # case: first character or lower character
            if (element_holder == "") or char.islower():
                element_holder += char
            # case: new element detected
            else:
                element_list.append(element_holder)
                element_holder = char

        # handling last element
        element_list.append(element_holder)

        return element_list
    
    def _expand_brackets(self, expression: str) -> Optional[str]:
        '''
        Expands the brackets based on the numbers present (iterative loop)
        Returns an expanded string
        '''
        # creating a copy
        text = expression
        expanding = True
        while expanding:
            search_results = re.search(self.bracket_regex, text)
            # case: no more hits
            if not search_results:
                break
            multiplier = search_results.group(2)
            ## to prevent integer overflow
            if len(multiplier) > 3:
                return None
            replacement = search_results.group(1) * int(multiplier)
            text = text.replace(search_results[0], replacement)

        return text


    def _split_expression(self, expression: str) -> Optional[List[str]]:
        """
        Splits an component's expression into strings and floats (still in string format) for further processing
        Removes the subscript tags before the splitting via regex
        """
        try:
            # removing all the natural spaces
            spaceless_expr = expression.replace(" ", "")

            # checking for and removing html tags and bracketed roman numerals
            clean_expr = re.sub(self.cleaning_regex, "", spaceless_expr)

            # translating all the subscript into numbers
            clean_expr_trans = clean_expr.translate(self.subs_trans)
            
            # print(f"{clean_expr_trans=}")

            if bool(re.search(self.discard_regex, clean_expr_trans)):
                return None

            # expanding the bracketed strings
            expanded_expr = self._expand_brackets(clean_expr_trans)
            if expanded_expr is None:
                return None

            if not self._passed_string_filters(expanded_expr):
                return None

            split_expr_list = [
                "".join(g) for _, g in itertools.groupby(expanded_expr, str.isalpha)
            ]
            # splitting up joined elements without subscripts (e.g. BFe)
            further_split_expr_list = []
            for x in split_expr_list:
                x = x.strip()
                if re.fullmatch(self.float_regex, x) or ("-" in x):
                    further_split_expr_list.append(x)
                elif x.isalpha():
                    mini_split = self._split_grouped_elements(x)
                    for y in mini_split:
                        further_split_expr_list.append(y)

            if not self._passed_list_filters(further_split_expr_list):
                return None
            
            # print(f"{further_split_expr_list=}")

            return further_split_expr_list


        except TypeError:
            return None

    def _split_grouped_elements(self, grouped_element: str) -> List[str]:
        """
        Splits up elements that escaped the splitting of the expression due to lack of subscript or number
        E.g. "BFe" should return ["B", "Fe"]
        Note that all elements start with an uppercase character
        """
        is_upper_case = False
        output_list = []
        holding_str = ""
        for char in grouped_element:
            # case: first character in list
            if char.isupper() and not is_upper_case:
                is_upper_case = True
                holding_str += char
            # case: hit another element
            elif char.isupper() and is_upper_case:
                output_list.append(holding_str)
                holding_str = char
            # case: still on same element
            elif char.islower():
                holding_str += char
        # append last holding_str
        output_list.append(holding_str)

        return output_list
    
    #### Filter Functions ####
    
    def _passed_string_filters(self, subs: str) -> bool:
        '''
        Filters through string-based logics
        Currently hardcoding the lists inside, to extract out when building the class
        '''
        ## case: start and end rejections
        if subs.startswith(self.start_rejects) or subs.endswith(self.end_rejects):
            return False

        ## case: counting brackets logic
        if subs.count("(") != subs.count(")"):
            return False

        ## case: all passed
        return True

    def _is_numeric(self, text:str) -> bool:
        '''
        Determines if the input is numeric or not (int or float)
        '''
        ## check for counts of "."
        if text.count(".") > 1:
            return False
        return text.replace(".", "").isnumeric()

    def _passed_list_filters(self, split_expr_list: list) -> bool:
        '''
        Filters through list-based logics
        Currently hardcoding some information inside, to extract when building the class
        '''
        list_len = len(split_expr_list)
        # case: empty list
        if len(split_expr_list) == 0:
            return False
        # case: list ends with 1 or 0
        if split_expr_list[-1] in ['1', '0']:
            return False
        # case: checking for same element sequence (except oxygen, cos COOH is common)
        if list_len > 1:
            curr_element = split_expr_list[0]
            for x in split_expr_list[1:]:
                if curr_element == x and x != "O":
                    return False
                else:
                    curr_element = x

        # special case: Carbon and less than 50 (because of fullerenes)
        if (list_len == 2):
            if (split_expr_list[0] == "C") and (self._is_numeric(split_expr_list[1])):
                if float(split_expr_list[1]) < 50:
                    return False

        # case: number same or larger than 100
        for x in split_expr_list:
            if self._is_numeric(x):
                if float(x) >= 100:
                    return False

        # case: all passed
        return True


class RubbishFilter():
    """
    Class for filtering all the rubbish stuff
    Rubbish defined to be:
    Single character substances
    Incomplete substance entities (numbers with symbols only)
    Fully caps stuff that were not normalised by the earlier substance_name step
    """
    def __init__(self):
        self.num_symbol_regex = re.compile(r"([-,'\[\]])?[N\d)(\[\]-]+([-\.,'\[\]])?[N,\d)(\[\]-]*?")
        self.full_caps_regex = re.compile(r"[A-Z]+s?")
        self.abbrev_list = ["PE", "PEEK", "NAFION", "PEDT", "KAPTON", "MYLAR"]
        self.chem_form_list = ["CO", "WC", "KOH", "HCN", "WO", "KCN"]
        self.wrong_list = ["SUS304", "PVAc"]
        self.combined_set = set(self.chem_form_list + self.abbrev_list)
    
    def filter_rubbish(self, subs_name: str) -> Optional[str]:
        '''
        Filtering function that combines the internal functions
        Returns the string itself (
        '''
        # controlling dict
        filtering_dict = {
            "Removing single character substances": self._is_single_char,
            "Removing incomplete subs (digits)": self._is_incomplete_digit,
            "Removing fully caps cases (with exceptions)": self._is_full_caps,
        }
        # running the gauntlet
        for step, fn in filtering_dict.items():
            if not fn(subs_name):
                continue
            else:
                return subs_name
        
        # case: did not hit any of the rubbish filters
        return None
        
    def _is_single_char(self, subs_name: str) -> bool:
        return len(subs_name) == 1

    def _is_incomplete_digit(self, subs_name: str) -> bool:
        return bool(re.fullmatch(self.num_symbol_regex, subs_name))
    
    def _is_full_caps(self, subs_name: str) -> bool:
        if subs_name in self.combined_set:
            return False
        if subs_name in self.wrong_list:
            return True
        return bool(re.fullmatch(self.full_caps_regex, subs_name))


class CasNumberNormalizer():
    """
    Class for normalising the CAS numbers
    Returns the CAS number with dashes removed (stored as int)
    Checksum checking implemented
    """
    def __init__(self):
        self.cas_regex = re.compile(r"\d{2,7}-\d{2,2}-\d")

    def extract_cas_num(self, subs_name: str) -> Optional[int]:
        ## case: matching the format of CAS number
        if bool(re.fullmatch(self.cas_regex, subs_name)):
            ## making a copy of the string
            subs_string = subs_name

            ## replacing the dashes to get pure number string
            number_string = subs_string.replace("-", "")
            number_list = [x for x in number_string]
            number_list.reverse()

            ## conduct the checking via the checksum
            try:
                mult_sum = sum([(i+1)*int(x) for i, x in enumerate(number_list[1:])])
                if int(number_list[0]) == mult_sum % 10:
                    return int(number_string)
            except:
                return None

        return None