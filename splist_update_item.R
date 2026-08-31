library(apde.etl)
library(Microsoft365R)

# Sharepoint auth
AzureAuth::get_azure_token(
  resource = "https://storage.azure.com", 
  tenant = keyring::key_get("adl_tenant", "dev"),
  app = keyring::key_get("adl_app", "dev"),
  auth_type = "authorization_code",
  use_cache = F
)

# set Sharepoint site
spsite <- get_sharepoint_site(site_url = "https://kc1.sharepoint.com/teams/DPH-Data-Collab-DataModernizationServices")
# set list
splist <- spsite$get_list("TestList")
# get all items
items <- splist$list_items()
# get only items where TestDate field is NULL
items <- splist$list_items(filter = "fields/TestDate eq null")
# get specific item by id
item <- splist$get_item(id = 1)
item$properties$fields$TestNumber
# updates item, adds 1 to TestNumber field and changes TestDate to current date
splist$update_item(id = 1, TestNumber = item$properties$fields$TestNumber + 1, TestDate = Sys.Date())


