##########################################################################
# Code to take in SafeGraph monthly PATTERNS data 
#  then convert to origin-destination pairs at ZIP code level
# Data downloaded on Jan. 5, 2022 for all 2018 to Nov 2021
# See web for metadata https://docs.safegraph.com/docs/monthly-patterns
# SafeGraph data is private and not included in this repo, 
#  please find in UT BOX or on TACC from me, Kelly P. or Jose Luis
##########################################################################

########################
##### Load libraries #####
########################
library(tidyverse)
library(tidycensus)

################################
##### Open and Read files #####
################################

zips_in_msa = read_csv("INPUT_DATA/zips_in_austin_rr_msa.csv")

# Define API key each time unless you save
# Use Sys.getenv("CENSUS_API_KEY") to check what it's defined as or create one
# Get total population by ZCTA from ACS 5-year avg 2015-2019, requires internet connection
pop = get_acs(geography="zcta", variables="B01001_001", geometry=F, year=2019) %>%
  select(GEOID, estimate) %>%
  rename(ORG_ZCTA=GEOID,
         ORG_ZCTA_POP=estimate)

# Translate CBG -> ZCTA
# All the hosp data will be from patient mailing addresses, i.e. ZIP codes, but the ZIP translation causes 
#  CBG to belong to too many ZIP 
# ZCTA is the polygon drawn around the ZIP code mail route, so they are almost the same but sometime off on edges
zcta_cbg_conv = read_csv("INPUT_DATA/ZCTA_CBG_MASTER_9_25_2020-tx.csv") %>% # 2020 Conversion table from Kelly G. from CBG to ZCTA
  select(cbg, ZCTA5CE10) %>%
  rename(ORG_CBG=cbg,
         ORG_ZCTA=ZCTA5CE10) %>%
  mutate(ORG_CBG = as.character(ORG_CBG),
         ORG_ZCTA = as.character(ORG_ZCTA)) %>%
  group_by(ORG_CBG) %>%
  slice(1) %>% # remove duplicates
  #mutate(proportion_in_zcta=1/n()) %>% # checks how many ZCTA a CBG belongs to => only 1 now
  ungroup() %>%
  left_join(pop, by="ORG_ZCTA")

# This only needed to be done once since my BASH code to do this task wasn't behaving
# # Unzip all the pattern files to csv and remove the zip files
# all_zip_files = list.files(pattern = "patterns.csv.gz$", recursive = TRUE)
# walk(all_zip_files, ~ R.utils::gunzip(.x, remove=T) )

# Get path to all patterns.csv files
# list.files preserves the order by date, but for later updates may need to remove the download date
all_pattern_files = list.files(pattern = "patterns.csv$", recursive = TRUE)
all_home_sum_files = list.files(pattern = "home_panel_summary.csv$", recursive = TRUE)

###################################
##### Clean up SafeGraph data #####
###################################

# Make folder for Monthly SafeGraph pattern output at ZIP level
# Also private but available in UT BOX
if(!dir.exists("Monthly_ZCTA_Patterns")){
  dir.create("Monthly_ZCTA_Patterns")
}else{print("dir exists :)")}

for(i in 1:length(all_pattern_files)){
  
  start_time=Sys.time() # start clock
  
  # Patters is the monthly flow of visitors to POI from their day time or night/home CBG
  patterns=
    read_csv(all_pattern_files[i]) %>%
    mutate(YEAR_MON=str_sub(date_range_start, start = 1, end=7) ) %>%
    select(postal_code, YEAR_MON, visitor_daytime_cbgs, visitor_home_cbgs ) %>%
    rename(DEST_ZCTA=postal_code,
           DAYTIME=visitor_daytime_cbgs,
           HOME=visitor_home_cbgs) %>%
    gather(key = VISITOR_TYPE, value = ORG_CBG, DAYTIME, HOME)  # Turn the 2 cbg columns into 1 with label
    
  ##### Need to get the total number of POI per ZCTA #####
  
  # Warning if for some reason there is more than 1 year-month combination in a monthly file
  year_month=unique(patterns$YEAR_MON)
  if(length(year_month)>1){
    print(paste0("Warning: year_month was expected to be 1 value but is", year_month ))
  }else{
    print(paste0(year_month, " started") )
  } # end if
  
  # Get the total devices tracked per CBG, need to get proportion of phones from each CBG traveling
  home_panel=
    read_csv(all_home_sum_files[i]) %>%
    mutate(YEAR_MON=as.Date(lubridate::ym(paste0(year, "-", month)), "%Y-%m"), # overly compicated way to get date in correct format
           YEAR_MON=str_sub(YEAR_MON, start = 1, end=7) ) %>% # want it to match YEAR_MON string above exactly
    rename(ORG_CBG=census_block_group,
           DAYTIME=number_devices_primary_daytime, # daytime 9am-5pm of devices could be same as home
           HOME=number_devices_residing) %>% # night time or home location of tracked devices
    gather(key = VISITOR_TYPE, value = TOTAL_DEVICES, DAYTIME, HOME) %>%
    select(YEAR_MON, ORG_CBG, VISITOR_TYPE, TOTAL_DEVICES) %>%
    left_join(zcta_cbg_conv, by="ORG_CBG") %>%
    drop_na() %>% # removes all the non-TX cbgs
    group_by(ORG_ZCTA) %>%
    mutate(ORG_ZCTA_TOTAL_DEVICES=sum(TOTAL_DEVICES)) %>%
    ungroup()
  
  # Convert CBG to ZIP for each org-dest pair
  # NUM_VISITOR is min 4 since SafeGraph will not report 0-1 visitors and 2-4 is always give as 4
  zcta_org_dest_pairs=
    separate_rows(patterns, ORG_CBG, sep=",", convert = TRUE) %>%
    mutate(ORG_CBG = str_replace_all(ORG_CBG, "\\{|\\}|\"", "")) %>%
    filter(!grepl("CA", ORG_CBG)) %>% # Remove all Canadian codes
    mutate(ORG_CBG = ifelse(ORG_CBG=="", NA, ORG_CBG) ) %>% # turn empty strings to NA
    drop_na() %>% # remove empty strings changed to NA
    separate(col=ORG_CBG, into=c("ORG_CBG", "NUM_VISITOR"), sep=":" ) %>% # split up data dict into 2 cols by ":"
    left_join(home_panel, by=c("YEAR_MON", "ORG_CBG", "VISITOR_TYPE") ) %>% # add on total visitors/pop per zcta
    drop_na() %>% # remove NA ORG_ZCTAs
    mutate(NUM_VISITOR=as.double(NUM_VISITOR)) %>%
    group_by(YEAR_MON, VISITOR_TYPE, ORG_ZCTA_POP, ORG_ZCTA, DEST_ZCTA, ORG_ZCTA_TOTAL_DEVICES) %>%
    summarise(ZCTA_NUM_VISITOR=sum(NUM_VISITOR)) %>% # Sum all the visitors of cbgs inside ZIP
    ungroup() %>% # not necessary but better to remove preserved grouping for speed
    filter(ORG_ZCTA %in% zips_in_msa$ZIP) # only want ZCTA that originate within the MSA

  #########################
  ##### Write to file #####
  #########################
  
  # write final origin destination pairs to file
  write.csv(zcta_org_dest_pairs, paste0("Monthly_ZCTA_Patterns/zcta_org_dest_pair_", year_month, ".csv"), row.names=F )
  
  end_time=Sys.time() # end clock
  total_time = end_time-start_time # get total elapsed time
  print(paste0(year_month, " finshed in ",  round(total_time, 0), " sec" ) )
  
} # end for loop over i, number of pattern files




