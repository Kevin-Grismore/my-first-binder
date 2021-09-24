import pandas as pd
from os import listdir
from os.path import isfile, join
import itertools

class Prep:
    '''
    State data standardization class.

    prep_all_states() calls each state's standardization method.
    _common() is the last step in every state's processing.
    '''
    
    def _common(cls, df, PATH):
        '''
            Transformation steps applied to every source file once it has been standardized.

            Parameters
            ----------
            df : Pandas.DataFrame
                The DataFrame to be transformed
            PATH : String
                The location of the file the DataFrame was sourced from

            Returns
            -------
            df : Pandas.DataFrame
                The prepared DataFrame, ready to be concatenated to all other processed data
        '''
        # Strip white space on string columns
        df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        
        # Fill 0s at the front of zips shorter than length 5
        df['Zip'] = df['Zip'].str.zfill(5)

        # Title case these columns in case they're not already
        to_title = ['FirstName', 'MiddleName', 'LastName', 'Street', 'City']
        
        for column in to_title:
            df[column] = df[column].str.title()
            
        # Drop duplicates in a single source file, if there are any
        df.drop_duplicates(inplace=True)

        # Make empty Hunt and Fish columns
        df['Hunt'] = ''
        df['Fish'] = ''
        
        # Determine whether this file contains hunting or fishing records based on the folder it's in
        df['Hunt'] = df['Hunt'].apply(lambda x: 'Y' if 'Hunt' in PATH else x)
        df['Fish'] = df['Fish'].apply(lambda x: 'Y' if 'Fish' in PATH else x)

        # Arrange columns for easy reading
        df = df.reindex(columns=['FirstName', 'MiddleName', 'LastName', 'Suffix', 'Street', 'City', 'State', 'Zip', 'Hunt', 'Fish'])

        return df
    
    def nebraska(cls, PATH):
        # Read in the source file. NE data is tab-separated .txt
        df = pd.read_csv(PATH, sep='\t', dtype=str)

        # Drop 'Sex' and 'email' columns if they're present
        # Some files have them and some don't, but they're not needed
        if 'Sex' in df:
            df.drop(columns=['Sex'], inplace=True)

        if 'email' in df:
            df.drop(columns=['email'], inplace=True)
        
        # Drop common columns about the hunting or fishing permit. Files are already categorized
        # by the folder they're in, so these aren't needed
        df.drop(columns=['permitYear', 'Permit Type', 'FullName'], inplace=True)
        
        # Rename columns to standard format
        df.rename(columns={'firstName':'FirstName', 'middleName':'MiddleName', 'lastName':'LastName', 'street':'Street', 'city':'City', 'state':'State', 'zip':'Zip'}, inplace=True)
        
        # Common processing
        df = cls._common(df, PATH)
        
        return df

    def north_dakota(cls, PATH):
        # Read in the source file. ND data is .xlsx with no headers, and some have multiple sheets ordered alphabetically
        # by last name. Collect and concatenate all sheets for a complete file
        df = pd.concat(pd.read_excel(PATH, sheet_name=None, header=None), ignore_index=True)
        
        # Give columns their names
        df.columns = ['LastName', 'FirstName', 'MiddleName', 'Street', 'City', 'State', 'Zip']
        
        # Common processing
        df = cls._common(df, PATH)

        return df

def prep_state(state_dir, method):
    '''
    Calls a single state's standardization method for each of its source files,
    and concatenates the results together into a single DataFrame.
    
    Parameters
    ----------
    state_dir : String
        The name of the state's source data folder
    method : String
        The name of the method that processes that state's data.
        
    Returns
    -------
    df : Pandas.DataFame
        A state's standardized source data
    '''
    # Get all the subfolders in a state folder (sould be Hunt and Fish)
    folders = [join(state_dir, f) for f in listdir(state_dir) if not isfile(join(state_dir, f))]
    
    # Get all the files in each of the state's subfolders
    files = []
    for folder in folders:
        files.append([join(folder, f) for f in listdir(folder) if isfile(join(folder, f))])
    
    # Convert all the file paths into a single list for easy iteration
    files = list(itertools.chain.from_iterable(files))
    
    # Instantiate the Prep class to call its methods by name
    prep = Prep()
    
    # Call the state's Prep method for each source file, and put all the returned
    # dataframes into a list
    dfs = []
    for file in files:
        dfs.append(getattr(prep, method)(file))
    
    # Concatenate all the dataframes in the list into a single dataframe
    df = pd.concat(dfs)
    
    # Drop duplicates. These will probably only exist if the same person has multiple licenses
    # of the same type
    df.drop_duplicates(inplace=True)
    
    return df

def prep_all_states():
    states = ['Nebraska', 'North Dakota']
    methods = ['nebraska', 'north_dakota']
    
    state_dfs = []
    for state, method in zip(states, methods):
        state_dfs.append(prep_state(state, method))
        
    df = pd.concat(state_dfs)
    
    return df