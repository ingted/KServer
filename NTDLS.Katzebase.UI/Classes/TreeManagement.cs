﻿using NTDLS.Katzebase.Client;
using static NTDLS.Katzebase.UI.Classes.Constants;

namespace NTDLS.Katzebase.UI.Classes
{
    public static class TreeManagement
    {
        public static void PopulateServer(TreeView treeView, string serverAddress)
        {
            using (var client = new KbClient(serverAddress, "Katzebase.UI"))
            {
                if (client.Server.Ping().Success == false)
                {
                    throw new Exception("Could not api ping the server.");
                }

                string key = serverAddress.ToLower();

                var foundNode = FindNodeOfType(treeView, ServerNodeType.Server, key);
                if (foundNode != null)
                {
                    treeView.Nodes.Remove(foundNode);
                }

                var serverNode = CreateServerNode(key, serverAddress);

                PopulateSchemaNode(serverNode, client, ":");

                treeView.Nodes.Add(serverNode);
            }
        }

        /// <summary>
        /// Populates a schema, its indexes and one level deeper to ensure there is somehting to expand in the tree.
        /// </summary>
        /// <param name="client"></param>
        /// <param name="name"></param>
        /// <returns></returns>
        private static void PopulateSchemaNode(ServerTreeNode nodeToPopulate, KbClient client, string schema)
        {
            var schemaIndexes = client.Schema.Indexes.List(schema);
            var schemaIndexesNode = CreateIndexFolderNode();
            foreach (var index in schemaIndexes.List)
            {
                schemaIndexesNode.Nodes.Add(CreateIndexNode(index.Name));
            }
            nodeToPopulate.Nodes.Add(schemaIndexesNode);

            var schemaFields = client.Document.List(schema, 1);
            var schemaFieldNode = CreateFieldFolderNode();
            foreach (var field in schemaFields.Fields)
            {
                schemaFieldNode.Nodes.Add(CreateFieldNode(field.Name));
            }
            nodeToPopulate.Nodes.Add(schemaFieldNode);

            var schemas = client.Schema.List(schema);
            foreach (var item in schemas.Collection)
            {
                var schemaNode = CreateSchemaNode(item.Name ?? "");
                schemaNode.Nodes.Add(CreateTreeNotLoadedNode());
                nodeToPopulate.Nodes.Add(schemaNode);
            }
        }

        public static void PopulateSchemaNodeOnExpand(TreeView treeView, ServerTreeNode node)
        {
            //We only populate nodes that do not contain schemas.
            if (node.Nodes.OfType<ServerTreeNode>().Where(o => o.NodeType == ServerNodeType.Schema).Any())
            {
                return;
            }

            var rootNode = GetRootNode(node);
            using (var client = new KbClient(rootNode.ServerAddress))
            {
                string schema = FullSchemaPath(node);

                node.Nodes.Clear(); //Dont clear the node until we hear back from the server.

                PopulateSchemaNode(node, client, schema);
            }
        }

        #region Tree node factories.

        public static ServerTreeNode CreateSchemaNode(string name)
        {
            var node = new ServerTreeNode(name)
            {
                NodeType = Constants.ServerNodeType.Schema,
                ImageKey = "Schema",
                SelectedImageKey = "Schema"
            };

            return node;
        }

        public static ServerTreeNode CreateServerNode(string name, string serverAddress)
        {
            var node = new ServerTreeNode(name)
            {
                NodeType = Constants.ServerNodeType.Server,
                ImageKey = "Server",
                SelectedImageKey = "Server",
                ServerAddress = serverAddress
            };

            return node;
        }

        public static ServerTreeNode CreateTreeNotLoadedNode()
        {
            var node = new ServerTreeNode("TreeNotLoaded")
            {
                NodeType = Constants.ServerNodeType.TreeNotLoaded,
                ImageKey = "TreeNotLoaded",
                SelectedImageKey = "TreeNotLoaded"
            };

            return node;
        }

        public static ServerTreeNode CreateIndexFolderNode()
        {
            var node = new ServerTreeNode("Indexes")
            {
                NodeType = Constants.ServerNodeType.IndexFolder,
                ImageKey = "IndexFolder",
                SelectedImageKey = "IndexFolder"
            };

            return node;
        }

        public static ServerTreeNode CreateFieldFolderNode()
        {
            var node = new ServerTreeNode("Fields")
            {
                NodeType = Constants.ServerNodeType.FieldFolder,
                ImageKey = "FieldFolder",
                SelectedImageKey = "FieldFolder"
            };

            return node;
        }

        public static ServerTreeNode CreateIndexNode(string name)
        {
            var node = new ServerTreeNode(name)
            {
                NodeType = Constants.ServerNodeType.Index,
                ImageKey = "Index",
                SelectedImageKey = "Index"
            };

            return node;
        }

        public static ServerTreeNode CreateFieldNode(string name)
        {
            var node = new ServerTreeNode(name)
            {
                NodeType = Constants.ServerNodeType.Field,
                ImageKey = "Field",
                SelectedImageKey = "Field"
            };

            return node;
        }

        #endregion

        public static string FullSchemaPath(ServerTreeNode node)
        {
            string result = string.Empty;

            if ((node as ServerTreeNode)?.NodeType == ServerNodeType.Schema)
            {
                result = node.Text;
            }

            while (node.Parent != null && (node.Parent as ServerTreeNode)?.NodeType != ServerNodeType.Schema)
            {
                node = (ServerTreeNode)node.Parent;
            }

            while (node.Parent != null && (node.Parent as ServerTreeNode)?.NodeType == ServerNodeType.Schema)
            {
                node = (ServerTreeNode)node.Parent;
                result = $"{node.Text}:{result}";
            }

            return result.Trim(new char[] { ':' });
        }

        public static ServerTreeNode GetRootNode(ServerTreeNode node)
        {
            while (node.Parent != null)
            {
                node = (ServerTreeNode)node.Parent;
            }
            return node;
        }

        public static ServerTreeNode? FindNodeOfType(TreeView treeView, ServerNodeType type, string text)
        {
            foreach (var node in treeView.Nodes.OfType<ServerTreeNode>())
            {
                var result = FindNodeOfType(node, type, text);
                if (result != null)
                {
                    return result;
                }

                if (node.NodeType == type && node.Text == text)
                {
                    return node;
                }
            }
            return null;
        }

        public static ServerTreeNode? FindNodeOfType(ServerTreeNode rootNode, ServerNodeType type, string text)
        {
            foreach (var node in rootNode.Nodes.OfType<ServerTreeNode>())
            {
                var result = FindNodeOfType(node, type, text);
                if (result != null)
                {
                    return result;
                }

                if (node.NodeType == type && node.Text == text)
                {
                    return node;
                }
            }
            return null;
        }

        public static int SortChildNodes(TreeNode node)
        {
            int moves = 0;

            for (int i = 0; i < node.Nodes.Count - 1; i++)
            {
                if (node.Nodes[i].Text.CompareTo(node.Nodes[i + 1].Text) > 0)
                {
                    int nodeIndex = node.Nodes[i].Index;
                    var nodeCopy = node.Nodes[i].Clone() as TreeNode;
                    KbUtility.EnsureNotNull(nodeCopy);
                    node.Nodes.Remove(node.Nodes[i]);

                    node.Nodes.Insert(nodeIndex + 1, nodeCopy);
                    moves++;
                }
                else if (node.Nodes[i + 1].Text.CompareTo(node.Nodes[i].Text) < 0)
                {
                    int nodeIndex = node.Nodes[i].Index;
                    var nodeCopy = node.Nodes[i].Clone() as TreeNode;
                    KbUtility.EnsureNotNull(nodeCopy);
                    node.Nodes.Remove(node.Nodes[i]);

                    node.Nodes.Insert(nodeIndex - 1, nodeCopy);
                    moves++;
                }
            }

            if (moves > 0)
            {
                return SortChildNodes(node);
            }

            return moves;
        }
    }
}
