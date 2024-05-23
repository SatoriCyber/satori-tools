## Satori CLI to Update Tableau Connections

This utility is designed to be run from a local CLI. It will:

- Report on Tableau connections and their Satori status
- Update a connection and generate a Satori Personal Access Token
- Update any connection manually for reversions, one-offs or similar.

**Install requirements**

```pip install -r requirements.txt```

**You must set the following environment variables.**

Tableau PAT info comes from Tableau admin area, Satori SA info comes from Satori admin area.

```
export satori_account_id=<satori_account_id>
export satori_sa_id=<satori_sa_id>
export satori_sa_key=<satori_sa_key>
export tableau_pat_name="Tableau Access Token Name"
export tableau_pat_secret=<Tableau Access Token Secret>
```

Also note: there is a python array ```event_data``` in satori_tableau_manager.py with some additional config settings. You should review this array for more information.


**Example commands**

```python satori_tableau_manager.py reportall```

This will generate an csv report for all connections in tableau! It will search for satoricyber.net URL's 
(you can edit the event parameter ```dac_search``` in ```satori_tableau_manager.py``` to refine this search)

___

```python satori_tableau_manager.py reportdatastore -datastore_id <A_SATORI_DS_ID>```

Same as the previous command, except for only one Satori Datastore (using its ID)

___

```
python satori_tableau_manager.py reportowner -owner <A_TABLEAU_EMAIL_OWNER> 
```

Reports on all content owned by a Tableau email. This report will attempt to use the search strings in ```dac_search``` to determine whether or not the content owned by a user is Satori-governed.

___

**With the above reporting info, you can now update individual connections in Tableau**


```
python satori_tableau_manager.py update_with_pat \
-content_id <TABLEAU_CONTENT_ID> \
-connection_id <TABLEAU_CONNECTION_ID> \
-datastore_id <SATORI_DATASTORE> \
-satori_new_pat_name <Desired new name for your new Satori PAT>

```

The above command will:
- Find the content in Tableau, and its owner email
- Verify that this email exists as a valid Satori user, and if yes
- Generate a new Satori Personal Access TokenName and TokenKey for this Satori user
- Update the Tableau Workbook or Datasource with this new info
- Change the hostname to the Satori hostname, using the Satori Datastore ID as reference

The command will exit without updating if no matching Satori user email is found for the specified Tableau Content Owner.

___ 

```python satori_tableau_manager.py manual``` will manually update any connection. To be used for reversions, undo's, or any type of connection update:

```
python satori_tableau_manager.py manual \
-content_id <TABLEAU_CONTENT_ID> \
-connection_id <TABLEAU_CONNECTION_ID> \
-newhostname <new hostname> \
-newusername <new username> \
-newpassword <new password> 

```

___

```
python satori_tableau_manager.py update_multiple_with_pat \
-owner <TABLEAU_EMAIL_OWNER> \
-datastore_id <SATORI_DATASTORE_ID> \
-satori_new_pat_name <DESIRED_NEW_PAT_NAME>
```

For a given Tableau Owner Email, combined with a given Satori Datastore ID, update all connections owned by this email address.

___

```
python satori_tableau_manager.py revert_multiple \
-owner <TABLEAU_EMAIL_OWNER> \
-datastore_id <SATORI_DATASTORE_ID> \
-newusername <reverted username> \
-newpassword <reverted password>
```

For a given Tableau Owner Email, combined with a given Satori Datastore ID, revert all connections owned by this email address. Requires manually providing a username and password.


___

Advanced: 

You can manually edit the ```event_data``` array in ```satori_tableau_manager.py``` to make additional environment changes, such as your Tableau "base URL", API version, and search strings.

