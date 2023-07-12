﻿using Katzebase.Engine.Atomicity;
using Katzebase.Engine.Schemas;
using Katzebase.PublicLibrary;
using static Katzebase.Engine.Library.EngineConstants;

namespace Katzebase.Engine.Documents.Management
{
    /// <summary>
    /// Public core class methods for locking, reading, writing and managing tasks related to documents.
    /// </summary>
    public class DocumentManager
    {
        private readonly Core core;
        internal DocumentQueryHandlers QueryHandlers { get; set; }
        public DocumentAPIHandlers APIHandlers { get; set; }

        public DocumentManager(Core core)
        {
            this.core = core;

            try
            {
                QueryHandlers = new DocumentQueryHandlers(core);
                APIHandlers = new DocumentAPIHandlers(core);
            }
            catch (Exception ex)
            {
                core.Log.Write("Failed to instanciate document manager.", ex);
                throw;
            }
        }

        /// <summary>
        /// When we want to read a document we do it here - no exceptions.
        /// </summary>
        /// <param name="transaction"></param>
        /// <param name="physicalSchema"></param>
        /// <param name="documentId"></param>
        internal PhysicalDocument AcquireDocument(Transaction transaction, PhysicalSchema physicalSchema, uint documentId, LockOperation lockIntention)
        {
            try
            {
                var documentPageCatalog = core.IO.GetJson<PhysicalDocumentPageCatalog>(transaction, physicalSchema.DocumentPageCatalogFilePath(), lockIntention);

                //Get the page that the document current exists in if any.
                var physicalPageMap = documentPageCatalog.GetDocumentPageMap(documentId);
                KbUtility.EnsureNotNull(physicalPageMap);

                //We found a page that contains the document, we need to open the page and modify the document with the given document id.
                var physicalDocumentPage = core.IO.GetJson<PhysicalDocumentPage>(transaction, physicalSchema.DocumentPageCatalogItemDiskPath(physicalPageMap), lockIntention);

                return physicalDocumentPage.Documents.First(o => o.Key == documentId).Value;
            }
            catch (Exception ex)
            {
                core.Log.Write($"Failed to acquire document for process {transaction.ProcessId}.", ex);
                throw;
            }
        }

        /// <summary>
        /// When we want to read a document we do it here. If we already have the page number, we can take a shortcut.
        /// </summary>
        /// <param name="transaction"></param>
        /// <param name="physicalSchema"></param>
        /// <param name="documentId"></param>
        internal PhysicalDocument AcquireDocument(Transaction transaction, PhysicalSchema physicalSchema, DocumentPointer documentPointer, LockOperation lockIntention)
        {
            try
            {
                var physicalDocumentPage = core.IO.GetJson<PhysicalDocumentPage>(transaction, physicalSchema.DocumentPageCatalogItemFilePath(documentPointer), lockIntention);
                return physicalDocumentPage.Documents.First(o => o.Key == documentPointer.DocumentId).Value;
            }
            catch (Exception ex)
            {
                core.Log.Write($"Failed to acquire document for process {transaction.ProcessId}.", ex);
                throw;
            }
        }

        internal IEnumerable<DocumentPointer> AcquireDocumentPointers(Transaction transaction, string schemaName, LockOperation lockIntention)
        {
            var physicalSchema = core.Schemas.Acquire(transaction, schemaName, LockOperation.Write);
            return AcquireDocumentPointers(transaction, physicalSchema, lockIntention);
        }

        internal IEnumerable<DocumentPointer> AcquireDocumentPointers(Transaction transaction, PhysicalSchema physicalSchema, LockOperation lockIntention)
        {
            try
            {
                var physicalDocumentPageCatalog = core.IO.GetJson<PhysicalDocumentPageCatalog>(transaction, physicalSchema.DocumentPageCatalogFilePath(), lockIntention);
                return physicalDocumentPageCatalog.ConsolidatedDocumentPointers();
            }
            catch (Exception ex)
            {
                core.Log.Write($"Failed to acquire document pointers for process {transaction.ProcessId}.", ex);
                throw;
            }
        }

        internal PhysicalDocumentPageCatalog AcquireDocumentPageCatalog(Transaction transaction, PhysicalSchema physicalSchema, LockOperation lockIntention)
        {
            try
            {
                return core.IO.GetJson<PhysicalDocumentPageCatalog>(transaction, physicalSchema.DocumentPageCatalogFilePath(), lockIntention);
            }
            catch (Exception ex)
            {
                core.Log.Write($"Failed to acquire document page catalog for process {transaction.ProcessId}.", ex);
                throw;
            }
        }

        /// <summary>
        /// When we want to create a document, this is where we do it - no exceptions.
        /// </summary>
        internal DocumentPointer InsertDocument(Transaction transaction, string schemaName, string pageContent)
        {
            var physicalSchema = core.Schemas.Acquire(transaction, schemaName, LockOperation.Write);
            return InsertDocument(transaction, physicalSchema, pageContent);

        }

        /// <summary>
        /// When we want to create a document, this is where we do it - no exceptions.
        /// </summary>
        internal DocumentPointer InsertDocument(Transaction transaction, PhysicalSchema physicalSchema, string pageContent)
        {
            try
            {
                PhysicalDocumentPage documentPage;

                //Open the document page catalog:
                var documentPageCatalog = core.IO.GetJson<PhysicalDocumentPageCatalog>(transaction, physicalSchema.DocumentPageCatalogFilePath(), LockOperation.Write);
                KbUtility.EnsureNotNull(documentPageCatalog);

                var physicalDocument = new PhysicalDocument
                {
                    Id = documentPageCatalog.ConsumeNextDocumentId(),
                    Content = pageContent,
                    Created = DateTime.UtcNow,
                    Modfied = DateTime.UtcNow,
                };

                //Find a page with some empty room:
                var physicalPageMap = documentPageCatalog.GetPageWithRoomForNewDocument(core.Settings.DocumentPageSize);

                if (physicalPageMap == null)
                {
                    //Still didnt find a page with room, we're going to have to create a new "page catalog item",
                    // add the given document ID to it and add that catalog item to the catalog collection:
                    physicalPageMap = new PhysicalDocumentPageMap(documentPageCatalog.NextPageNumber());
                    physicalPageMap.DocumentIDs.Add(physicalDocument.Id);

                    //We created a new page item, add it to the catalog:
                    documentPageCatalog.PageMappings.Add(physicalPageMap);

                    //Create the new page, this will store the actual document contents.
                    documentPage = new PhysicalDocumentPage(physicalPageMap.PageNumber);

                    //Add the given document to the page document.
                    documentPage.Documents.Add(physicalDocument.Id, physicalDocument);
                }
                else
                {
                    //We found a page with space, just add the document ID to the page catalog item.
                    physicalPageMap.DocumentIDs.Add(physicalDocument.Id);

                    //Open the page and add the document to it.
                    documentPage = core.IO.GetJson<PhysicalDocumentPage>(transaction, physicalSchema.DocumentPageCatalogItemDiskPath(physicalPageMap), LockOperation.Write);

                    //Add the given document to the page document.
                    documentPage.Documents.Add(physicalDocument.Id, physicalDocument);
                }

                //Save the document page:
                core.IO.PutJson(transaction, physicalSchema.DocumentPageCatalogItemDiskPath(physicalPageMap), documentPage);

                //Save the docuemnt page catalog:
                core.IO.PutJson(transaction, physicalSchema.DocumentPageCatalogFilePath(), documentPageCatalog);

                var documentPointer = new DocumentPointer(documentPage.PageNumber, physicalDocument.Id);

                //Update all of the indexes that referecne the document.
                core.Indexes.InsertDocumentIntoIndexes(transaction, physicalSchema, physicalDocument, documentPointer);

                return documentPointer;
            }
            catch (Exception ex)
            {
                core.Log.Write($"Failed to insert document for process {transaction.ProcessId}.", ex);
                throw;
            }
        }

        /// <summary>
        /// When we want to update multiple documents in the same schema, this is where we do it - no exceptions.
        /// </summary>
        /// <param name="transaction"></param>
        /// <param name="physicalSchema"></param>
        /// <param name="documents">List of dovuemtn pointers and their new content.</param>
        /// <param name="listOfModifiedFields">A list of the fields that were modified so that we can filter the indexes we need to update.</param>
        internal void UpdateDocuments(Transaction transaction, PhysicalSchema physicalSchema,
            Dictionary<DocumentPointer, string> documents, IEnumerable<string>? listOfModifiedFields = null)
        {
            try
            {
                var physicalDocuments = new Dictionary<DocumentPointer, PhysicalDocument>();

                foreach (var document in documents)
                {
                    var documentPage = core.IO.GetJson<PhysicalDocumentPage>(transaction, physicalSchema.DocumentPageCatalogItemFilePath(document.Key), LockOperation.Write);

                    var physicalDocument = new PhysicalDocument()
                    {
                        Id = document.Key.DocumentId,
                        Content = document.Value,
                        Created = documentPage.Documents[document.Key.DocumentId].Created,
                        Modfied = DateTime.UtcNow
                    };

                    documentPage.Documents[document.Key.DocumentId] = physicalDocument;

                    //Save the document page:
                    core.IO.PutJson(transaction, physicalSchema.DocumentPageCatalogItemFilePath(document.Key), documentPage);

                    physicalDocuments.Add(document.Key, physicalDocument);
                }

                //Update all of the indexes that referecne the document.
                core.Indexes.UpdateDocumentsIntoIndexes(transaction, physicalSchema, physicalDocuments, listOfModifiedFields);
            }
            catch (Exception ex)
            {
                core.Log.Write($"Failed to update document for process {transaction.ProcessId}.", ex);
                throw;
            }
        }

        /// <summary>
        /// When we want to update a document, this is where we do it - no exceptions.
        /// </summary>
        /// <param name="transaction"></param>
        /// <param name="schema"></param>
        /// <param name="document"></param>
        internal void DeleteDocuments(Transaction transaction, PhysicalSchema physicalSchema, IEnumerable<DocumentPointer> documentPointers)
        {
            try
            {
                //Open the document page catalog:
                var documentPageCatalog = core.IO.GetJson<PhysicalDocumentPageCatalog>(transaction, physicalSchema.DocumentPageCatalogFilePath(), LockOperation.Write);
                KbUtility.EnsureNotNull(documentPageCatalog);

                foreach (var documentPointer in documentPointers)
                {
                    var documentPage = core.IO.GetJson<PhysicalDocumentPage>(transaction, physicalSchema.DocumentPageCatalogItemFilePath(documentPointer), LockOperation.Write);

                    //Remove the item from the document page.
                    documentPage.Documents.Remove(documentPointer.DocumentId);

                    //Save the document page:
                    core.IO.PutJson(transaction, physicalSchema.DocumentPageCatalogItemFilePath(documentPointer), documentPage);

                    documentPageCatalog.PageMappings[documentPointer.PageNumber].DocumentIDs.Remove(documentPointer.DocumentId);

                    //Save the docuemnt page catalog:
                    core.IO.PutJson(transaction, physicalSchema.DocumentPageCatalogFilePath(), documentPageCatalog);
                }

                //Update all of the indexes that referecne the documents.
                core.Indexes.RemoveDocumentsFromIndexes(transaction, physicalSchema, documentPointers);
            }
            catch (Exception ex)
            {
                core.Log.Write($"Failed to delete documents for process {transaction.ProcessId}.", ex);
                throw;
            }
        }
    }
}
