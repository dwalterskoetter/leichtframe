using System.Reflection;
using System.Text;
using System.Xml.Linq;

namespace LeichtFrame.DocGen
{
    class Program
    {
        static readonly HashSet<string> _ignoredTypes = new()
        {
            "LeichtFrame.Core.Column",
            "LeichtFrame.Core.Column`1",
            "LeichtFrame.Core.ColumnFactory",
            "LeichtFrame.IO.ArrowConverter",
            "LeichtFrame.Benchmarks",
            "LeichtFrame.Core.Tests",
            "LeichtFrame.IO.Tests"
        };

        static readonly List<string> _searchDirectories = new();

        static void Main(string[] args)
        {
            var root = Directory.GetCurrentDirectory();

            var inputs = new[]
            {
                new { Dll = "artifacts/doc_bin/LeichtFrame.Core.dll", Name = "LeichtFrame.Core" },
                new { Dll = "artifacts/doc_bin/LeichtFrame.IO.dll", Name = "LeichtFrame.IO" }
            };

            foreach (var input in inputs)
            {
                var fullPath = Path.GetFullPath(Path.Combine(root, input.Dll));
                var dir = Path.GetDirectoryName(fullPath);

                if (dir != null && !_searchDirectories.Contains(dir))
                {
                    _searchDirectories.Add(dir);
                }
            }

            AppDomain.CurrentDomain.AssemblyResolve += ResolveAssembly;

            var outputBaseDir = Path.Combine(root, "website/docs");
            var generatedRootDir = Path.Combine(outputBaseDir, "LeichtFrame");

            if (Directory.Exists(generatedRootDir)) Directory.Delete(generatedRootDir, true);

            Console.WriteLine("🚀 Starting Pure .NET Doc Generator...");

            foreach (var input in inputs)
            {
                var dllPath = Path.Combine(root, input.Dll);
                var xmlPath = Path.ChangeExtension(dllPath, ".xml");

                if (!File.Exists(dllPath))
                {
                    Console.WriteLine($"❌ DLL missing: {dllPath} (Run generate_docs.sh first!)");
                    continue;
                }

                PreloadDependencies(Path.GetDirectoryName(dllPath));

                var docMap = LoadXmlDocumentation(xmlPath);

                Assembly assembly;
                try
                {
                    assembly = Assembly.LoadFrom(dllPath);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"❌ Critical: Could not load assembly {input.Name}: {ex.Message}");
                    continue;
                }

                var relativePath = input.Name.Replace('.', Path.DirectorySeparatorChar);
                var targetDir = Path.Combine(outputBaseDir, relativePath);

                Directory.CreateDirectory(targetDir);

                List<Type> types;
                try
                {
                    types = assembly.GetExportedTypes()
                        .Where(t => !t.Name.StartsWith("<") && !t.IsSpecialName)
                        .Where(t => !_ignoredTypes.Contains(t.FullName!) && t.GetCustomAttribute<ObsoleteAttribute>() == null)
                        .OrderBy(t => t.Name)
                        .ToList();
                }
                catch (ReflectionTypeLoadException ex)
                {
                    Console.WriteLine($"⚠️ Warning: Partial load for {input.Name}. Missing dependencies?");
                    types = ex.Types.Where(t => t != null
                        && !t.Name.StartsWith("<")
                        && !t.IsSpecialName
                        && !_ignoredTypes.Contains(t.FullName!))
                        .Select(t => t!).ToList();
                }

                GenerateNamespaceIndex(input.Name, types, targetDir, docMap);

                foreach (var type in types)
                {
                    GenerateTypeDoc(type, targetDir, relativePath, docMap);
                }
            }
            Console.WriteLine("✅ Done.");
        }

        static Assembly? ResolveAssembly(object? sender, ResolveEventArgs args)
        {
            if (args.Name.Contains(".resources")) return null;

            var assemblyName = new AssemblyName(args.Name).Name;

            var loaded = AppDomain.CurrentDomain.GetAssemblies()
                .FirstOrDefault(a => a.GetName().Name == assemblyName);
            if (loaded != null) return loaded;

            var fileName = assemblyName + ".dll";
            foreach (var dir in _searchDirectories)
            {
                var potentialPath = Path.Combine(dir, fileName);
                if (File.Exists(potentialPath))
                {
                    try { return Assembly.LoadFrom(potentialPath); } catch { }
                }
            }
            return null;
        }

        static void PreloadDependencies(string? directory)
        {
            if (string.IsNullOrEmpty(directory) || !Directory.Exists(directory)) return;

            foreach (var dll in Directory.GetFiles(directory, "*.dll"))
            {
                try
                {
                    var name = AssemblyName.GetAssemblyName(dll).Name;
                    if (!AppDomain.CurrentDomain.GetAssemblies().Any(a => a.GetName().Name == name))
                    {
                        Assembly.LoadFrom(dll);
                    }
                }
                catch { }
            }
        }

        static void GenerateNamespaceIndex(string namespaceName, List<Type> types, string outputDir, Dictionary<string, string> docMap)
        {
            var sb = new StringBuilder();
            var shortName = namespaceName.Contains('.') ? namespaceName.Substring(namespaceName.LastIndexOf('.') + 1) : namespaceName;

            sb.AppendLine("---");
            sb.AppendLine($"sidebar_label: {shortName}");
            sb.AppendLine($"title: {namespaceName}");
            sb.AppendLine($"sidebar_position: 0");
            sb.AppendLine("---");
            sb.AppendLine();
            sb.AppendLine($"# Namespace {namespaceName}");
            sb.AppendLine();
            sb.AppendLine("Contains the following types:");
            sb.AppendLine();
            sb.AppendLine("| Type | Description |");
            sb.AppendLine("| --- | --- |");

            foreach (var t in types)
            {
                string typeId = $"T:{t.FullName}";
                string summary = docMap.ContainsKey(typeId) ? docMap[typeId] : "";

                var firstSentence = summary.Split('.')[0];
                if (!string.IsNullOrWhiteSpace(firstSentence)) firstSentence += ".";

                string displayName = ToMdxSafe(CleanTypeName(t));
                string safeFilename = GetSafeFilename(t);

                sb.AppendLine($"| [{displayName}](./{safeFilename}.md) | {ToMdxSafe(firstSentence)} |");
            }

            File.WriteAllText(Path.Combine(outputDir, "index.md"), sb.ToString());
            Console.WriteLine($"   Generated Index: {namespaceName}/index.md");
        }

        static void GenerateTypeDoc(Type type, string outputDir, string relativePath, Dictionary<string, string> docMap)
        {
            var sb = new StringBuilder();
            string readableName = CleanTypeName(type);
            string mdxSafeName = ToMdxSafe(readableName);

            sb.AppendLine("---");
            sb.AppendLine($"sidebar_label: {readableName}");
            sb.AppendLine($"title: {readableName}");
            sb.AppendLine("---");
            sb.AppendLine();
            sb.AppendLine($"# {mdxSafeName}");
            sb.AppendLine($"**Namespace:** `{type.Namespace}`");
            sb.AppendLine();

            string typeId = $"T:{type.FullName}";
            if (docMap.TryGetValue(typeId, out var summary))
            {
                sb.AppendLine(summary);
                sb.AppendLine();
            }

            var props = type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly);
            if (props.Any())
            {
                sb.AppendLine("## Properties");
                sb.AppendLine("| Name | Type | Description |");
                sb.AppendLine("| --- | --- | --- |");

                foreach (var p in props)
                {
                    string propId = $"P:{type.FullName}.{p.Name}";
                    string doc = docMap.ContainsKey(propId) ? docMap[propId] : "";
                    string typeName = CleanTypeName(p.PropertyType);

                    string propName = p.Name;
                    var indexParams = p.GetIndexParameters();
                    if (indexParams.Length > 0)
                    {
                        var args = string.Join(", ", indexParams.Select(ip => CleanTypeName(ip.ParameterType)));
                        propName = $"this[{args}]";
                    }

                    sb.AppendLine($"| `{propName}` | `{typeName}` | {doc} |");
                }
                sb.AppendLine();
            }

            var methods = type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)
                .Where(m => !m.IsSpecialName && !m.Name.StartsWith("<")
                        && m.Name != "GetType" && m.Name != "ToString" && m.Name != "Equals"
                        && m.Name != "GetHashCode" && m.Name != "Deconstruct")
                .OrderBy(m => m.Name);

            if (methods.Any())
            {
                sb.AppendLine("## Methods");
                foreach (var m in methods)
                {
                    try
                    {
                        var safeMethodName = ToMdxSafe(m.Name);
                        sb.AppendLine($"### {safeMethodName}");

                        var parameters = m.GetParameters().Select(p =>
                        {
                            string prefix = "";
                            if (p.IsOut) prefix = "out ";
                            else if (p.ParameterType.IsByRef) prefix = "ref ";
                            if (p.GetCustomAttribute<ParamArrayAttribute>() != null) prefix += "params ";

                            return $"{prefix}{CleanTypeName(p.ParameterType)} {p.Name}";
                        });

                        var paramStr = string.Join(", ", parameters);
                        var returnType = CleanTypeName(m.ReturnType);
                        var modifiers = m.IsStatic ? "static " : "";

                        sb.AppendLine($"```csharp\npublic {modifiers}{returnType} {m.Name}({paramStr})\n```");

                        string methodId = GetMethodId(m);
                        if (docMap.TryGetValue(methodId, out var mDoc))
                        {
                            sb.AppendLine(mDoc);
                        }
                        sb.AppendLine();
                    }
                    catch (Exception ex)
                    {
                        sb.AppendLine($"> ⚠️ **Documentation unavailable:** {ex.Message} (Dependency missing?)");
                        sb.AppendLine();
                    }
                }
            }

            string safeFilename = GetSafeFilename(type);
            File.WriteAllText(Path.Combine(outputDir, $"{safeFilename}.md"), sb.ToString());
            Console.WriteLine($"   Generated: {relativePath}/{safeFilename}.md");
        }

        static string GetSafeFilename(Type t)
        {
            if (t.IsGenericType)
            {
                var cleanName = t.Name.Split('`')[0];
                return cleanName + "T";
            }
            return t.Name;
        }

        static string ToMdxSafe(string text)
        {
            if (string.IsNullOrEmpty(text)) return text;
            return text.Replace("<", "&lt;").Replace(">", "&gt;").Replace("`", "");
        }

        static Dictionary<string, string> LoadXmlDocumentation(string xmlPath)
        {
            var map = new Dictionary<string, string>();
            if (!File.Exists(xmlPath)) return map;

            try
            {
                var doc = XDocument.Load(xmlPath);
                foreach (var member in doc.Descendants("member"))
                {
                    var name = member.Attribute("name")?.Value;
                    var summaryNode = member.Element("summary");

                    if (name != null && summaryNode != null)
                    {
                        map[name] = XmlToMarkdown(summaryNode);
                    }
                }
            }
            catch { }
            return map;
        }

        static string XmlToMarkdown(XElement element)
        {
            var sb = new StringBuilder();

            foreach (var node in element.Nodes())
            {
                if (node is XText textNode)
                {
                    sb.Append(textNode.Value);
                }
                else if (node is XElement el)
                {
                    if (el.Name == "see")
                    {
                        var cref = el.Attribute("cref")?.Value ?? "";
                        var shortName = cref.Split('.').Last();
                        if (shortName.Contains(':')) shortName = shortName.Split(':')[1];
                        if (shortName.Contains('`')) shortName = shortName.Split('`')[0] + "<T>";
                        sb.Append($"`{shortName}`");
                    }
                    else if (el.Name == "paramref")
                    {
                        var name = el.Attribute("name")?.Value ?? "";
                        sb.Append($"`{name}`");
                    }
                    else if (el.Name == "c" || el.Name == "code")
                    {
                        sb.Append($"`{el.Value}`");
                    }
                    else if (el.Name == "para")
                    {
                        sb.AppendLine();
                        sb.AppendLine(XmlToMarkdown(el));
                        sb.AppendLine();
                    }
                    else
                    {
                        sb.Append(XmlToMarkdown(el));
                    }
                }
            }
            return sb.ToString().Trim();
        }

        static string GetMethodId(MethodInfo m)
        {
            var sb = new StringBuilder();
            sb.Append("M:");
            sb.Append(m.DeclaringType?.FullName);
            sb.Append(".");
            sb.Append(m.Name);

            if (m.IsGenericMethod)
            {
                sb.Append("``");
                sb.Append(m.GetGenericArguments().Length);
            }

            var parameters = m.GetParameters();
            if (parameters.Length > 0)
            {
                sb.Append("(");
                for (int i = 0; i < parameters.Length; i++)
                {
                    if (i > 0) sb.Append(",");
                    var pType = parameters[i].ParameterType;

                    if (pType.IsGenericParameter)
                    {
                        sb.Append($"``{pType.GenericParameterPosition}");
                    }
                    else
                    {
                        sb.Append(pType.FullName?.Replace("&", "") ?? pType.Name.Replace("&", ""));
                    }
                }
                sb.Append(")");
            }
            return sb.ToString();
        }

        static string CleanTypeName(Type t)
        {
            if (t.IsByRef) t = t.GetElementType()!;

            if (t == typeof(int)) return "int";
            if (t == typeof(string)) return "string";
            if (t == typeof(bool)) return "bool";
            if (t == typeof(double)) return "double";
            if (t == typeof(void)) return "void";
            if (t == typeof(object)) return "object";

            if (t.IsGenericType)
            {
                var name = t.Name.Split('`')[0];
                var args = string.Join(", ", t.GetGenericArguments().Select(CleanTypeName));
                return $"{name}<{args}>";
            }

            return t.Name.Split('`')[0].Replace("&", "");
        }

        static string CleanDoc(string doc) => doc.Replace("\n", " ").Trim();
    }
}