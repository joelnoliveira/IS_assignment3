source 1 and 2 modes were changed from "bulk" to "incrementing", this way and assuming there will be only insert operations (not update nor delete), only new data will be printed, instead of printing every content of the database at every 60 seconds. Also there was a conflict and the query needed removal, instead added table.whitelist


<!--TODO Changed to incrementing to avoid duplicates, but after each restart duplicate values appear, becarefull with that  -->