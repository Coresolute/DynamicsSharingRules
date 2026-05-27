using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Crm.Sdk.Messages;

namespace DynamicsCustomSharing
{
    public class ExecuteOwnerBasedSharing : IPlugin
    {
        public void Execute(IServiceProvider serviceProvider)
        {
            IPluginExecutionContext context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            IOrganizationServiceFactory serviceFactory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            IOrganizationService service = serviceFactory.CreateOrganizationService(context.UserId);
            ITracingService tracingService = (ITracingService)serviceProvider.GetService(typeof(ITracingService));

            try
            {
                tracingService.Trace("========== PLUGIN EXECUTION STARTED ==========");

                if (!context.InputParameters.Contains("Target") || !(context.InputParameters["Target"] is Entity))
                {
                    tracingService.Trace("EXITING: No Target entity found in pipeline.");
                    return;
                }

                Entity targetPayload = (Entity)context.InputParameters["Target"];
                tracingService.Trace($"Target ID intercepted: {targetPayload.Id}");

                
                tracingService.Trace("Fetching the complete record directly from the database...");
                Entity sharingRequest = service.Retrieve(
                    targetPayload.LogicalName,
                    targetPayload.Id,
                    new ColumnSet("statuscode", "cr6da_sharetype", "cr6da_selectentities", "new_recordownby", "new_recordssharewith")
                );

                
                int submittedStatusCode = 1;
                tracingService.Trace($"Checking statuscode. Expected 'Submitted' value: {submittedStatusCode}");

                if (!sharingRequest.Contains("statuscode"))
                {
                    tracingService.Trace("EXITING: 'statuscode' is empty in the database.");
                    return;
                }

                OptionSetValue status = sharingRequest.GetAttributeValue<OptionSetValue>("statuscode");
                tracingService.Trace($"Database Status Value: {status.Value}.");

                if (status.Value != submittedStatusCode)
                {
                    tracingService.Trace("EXITING: Status value does not match 'Submitted'.");
                    return;
                }
                tracingService.Trace("Status is correct. Proceeding to variable extraction...");

                
                tracingService.Trace("Extracting 'cr6da_sharetype'...");
                if (!sharingRequest.Contains("cr6da_sharetype"))
                {
                    tracingService.Trace("WARNING: 'cr6da_sharetype' is empty in the database. Defaulting to ReadAccess.");
                }

                OptionSetValue shareTypeChoice = sharingRequest.GetAttributeValue<OptionSetValue>("cr6da_sharetype");
                tracingService.Trace($"Raw Share Type Choice Value: {(shareTypeChoice != null ? shareTypeChoice.Value.ToString() : "NULL")}");

                bool isRevokeAction = IsRevokeAction(shareTypeChoice?.Value);
                AccessRights grantedAccess = MapShareTypeToAccessRights(shareTypeChoice?.Value);
                tracingService.Trace($"Action Evaluated -> Is Revoke: {isRevokeAction} | Granted Access Mask: {grantedAccess}");

                
                tracingService.Trace("Extracting 'cr6da_selectentities'...");
                if (!sharingRequest.Contains("cr6da_selectentities"))
                {
                    tracingService.Trace("EXITING: 'cr6da_selectentities' is empty in the database.");
                    return;
                }

                OptionSetValueCollection selectedEntities = sharingRequest.GetAttributeValue<OptionSetValueCollection>("cr6da_selectentities");

                if (selectedEntities == null || selectedEntities.Count == 0)
                {
                    tracingService.Trace("EXITING: 'cr6da_selectentities' contains 0 choices.");
                    return;
                }

                tracingService.Trace($"Found {selectedEntities.Count} entity choice(s). Mapping to logical names...");
                List<string> targetEntities = new List<string>();
                foreach (OptionSetValue choice in selectedEntities)
                {
                    string logicalName = MapChoiceToLogicalName(choice.Value);
                    if (!string.IsNullOrEmpty(logicalName))
                    {
                        targetEntities.Add(logicalName);
                        tracingService.Trace($" - Mapped Value {choice.Value} to: '{logicalName}'");
                    }
                    else
                    {
                        tracingService.Trace($" - WARNING: Could not map Value {choice.Value}. It was ignored.");
                    }
                }

                if (targetEntities.Count == 0)
                {
                    tracingService.Trace("EXITING: No valid entities were mapped from the choices.");
                    return;
                }

                
                tracingService.Trace("Extracting User text fields...");
                string sourceOwnersStr = sharingRequest.GetAttributeValue<string>("new_recordownby");
                string targetShareWithStr = sharingRequest.GetAttributeValue<string>("new_recordssharewith");

                tracingService.Trace($"Raw 'new_recordownby' string: {(string.IsNullOrEmpty(sourceOwnersStr) ? "EMPTY" : sourceOwnersStr)}");
                tracingService.Trace($"Raw 'new_recordssharewith' string: {(string.IsNullOrEmpty(targetShareWithStr) ? "EMPTY" : targetShareWithStr)}");

                
                List<Guid> sourceOwnerIds = ResolveUserNamesToGuids(service, tracingService, sourceOwnersStr);
                List<Guid> targetShareWithIds = ResolveUserNamesToGuids(service, tracingService, targetShareWithStr);

                tracingService.Trace($"Successfully Resolved Source Owners: {sourceOwnerIds.Count} valid GUID(s)");
                tracingService.Trace($"Successfully Resolved Target Share With: {targetShareWithIds.Count} valid GUID(s)");

                if (!sourceOwnerIds.Any() || !targetShareWithIds.Any())
                {
                    tracingService.Trace("CRASHING: One or both user lists resolved to 0 valid GUIDs.");
                    throw new InvalidPluginExecutionException("Sharing failed: We could not find active Dataverse users matching the exact names provided.");
                }

                
                tracingService.Trace("Initializing bulk execution batch...");
                ExecuteMultipleRequest bulkShareRequest = new ExecuteMultipleRequest()
                {
                    Settings = new ExecuteMultipleSettings() { ContinueOnError = true, ReturnResponses = true },
                    Requests = new OrganizationRequestCollection()
                };

                
                foreach (string entityLogicalName in targetEntities)
                {
                    tracingService.Trace($"--- Querying {entityLogicalName} records owned by the {sourceOwnerIds.Count} source user(s) ---");

                    QueryExpression query = new QueryExpression(entityLogicalName)
                    {
                        ColumnSet = new ColumnSet(entityLogicalName + "id")
                    };
                    query.Criteria.AddCondition("ownerid", ConditionOperator.In, sourceOwnerIds.Cast<object>().ToArray());

                    EntityCollection recordsToProcess = service.RetrieveMultiple(query);
                    tracingService.Trace($"Query complete. Found {recordsToProcess.Entities.Count} {entityLogicalName} record(s) to process.");

                    foreach (Entity record in recordsToProcess.Entities)
                    {
                        foreach (Guid targetUserId in targetShareWithIds)
                        {
                            OrganizationRequest dynamicRequest;

                            if (isRevokeAction)
                            {
                                dynamicRequest = new RevokeAccessRequest
                                {
                                    Target = new EntityReference(record.LogicalName, record.Id),
                                    Revokee = new EntityReference("systemuser", targetUserId)
                                };
                            }
                            else
                            {
                                dynamicRequest = new GrantAccessRequest
                                {
                                    Target = new EntityReference(record.LogicalName, record.Id),
                                    PrincipalAccess = new PrincipalAccess
                                    {
                                        Principal = new EntityReference("systemuser", targetUserId),
                                        AccessMask = grantedAccess
                                    }
                                };
                            }

                            bulkShareRequest.Requests.Add(dynamicRequest);

                            if (bulkShareRequest.Requests.Count >= 1000)
                            {
                                tracingService.Trace("Batch hit 1000 requests. Executing batch...");
                                service.Execute(bulkShareRequest);
                                bulkShareRequest.Requests.Clear();
                                tracingService.Trace("Batch execution successful. Cleared batch.");
                            }
                        }
                    }
                }

                if (bulkShareRequest.Requests.Count > 0)
                {
                    tracingService.Trace($"Executing final batch of {bulkShareRequest.Requests.Count} request(s)...");
                    service.Execute(bulkShareRequest);
                    tracingService.Trace("Final batch execution successful.");
                }

                tracingService.Trace("========== PLUGIN EXECUTION FINISHED SUCCESSFULLY ==========");
            }
            catch (Exception ex)
            {
                tracingService.Trace($"FATAL ERROR: {ex.Message}");
                tracingService.Trace($"STACK TRACE: {ex.StackTrace}");
                throw new InvalidPluginExecutionException($"Sharing Engine Failed: {ex.Message}");
            }
        }

        
        private List<Guid> ResolveUserNamesToGuids(IOrganizationService service, ITracingService tracingService, string namesString)
        {
            List<Guid> resolvedGuids = new List<Guid>();
            if (string.IsNullOrWhiteSpace(namesString)) return resolvedGuids;

            
            string[] namesArray = namesString.Split(',');

            foreach (string name in namesArray)
            {
                string trimmedName = name.Trim();
                if (string.IsNullOrEmpty(trimmedName)) continue;

                tracingService.Trace($"Attempting to resolve user name: '{trimmedName}'");

                
                QueryExpression query = new QueryExpression("systemuser")
                {
                    ColumnSet = new ColumnSet("systemuserid"),
                    TopCount = 1
                };
                query.Criteria.AddCondition("fullname", ConditionOperator.Equal, trimmedName);
                query.Criteria.AddCondition("isdisabled", ConditionOperator.Equal, false);

                EntityCollection results = service.RetrieveMultiple(query);

                if (results.Entities.Count > 0)
                {
                    Guid foundId = results.Entities[0].Id;
                    resolvedGuids.Add(foundId);
                    tracingService.Trace($" -> Successfully resolved '{trimmedName}' to ID: {foundId}");
                }
                else
                {
                    tracingService.Trace($" -> WARNING: Could not find an active user named '{trimmedName}' in the system.");
                }
            }

            return resolvedGuids;
        }

        private string MapChoiceToLogicalName(int choiceValue)
        {
            switch (choiceValue)
            {
                case 679000000: return "contact";
                case 679000001: return "lead";
                case 679000002: return "opportunity";
                default: return null;
            }
        }

        private bool IsRevokeAction(int? choiceValue)
        {
            int revokeChoiceValue = 100000001;
            return choiceValue.HasValue && choiceValue.Value == revokeChoiceValue;
        }

        private AccessRights MapShareTypeToAccessRights(int? choiceValue)
        {
            if (!choiceValue.HasValue) return AccessRights.ReadAccess;

            switch (choiceValue.Value)
            {
                case 679000000: // Read
                    return AccessRights.ReadAccess;
                case 679000001: // Write 
                    return AccessRights.WriteAccess | AccessRights.ReadAccess;
                case 679000002: // Read/Write
                    return AccessRights.ReadAccess | AccessRights.WriteAccess;
                case 100000001: // Revoke
                    return AccessRights.None;
                default:
                    return AccessRights.ReadAccess;
            }
        }
    }
}