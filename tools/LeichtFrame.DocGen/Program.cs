using System.Reflection;
using System.Text;
using System.Xml.Linq;

namespace LeichtFrame.DocGen
{
    class Program
    {
        static readonly List<string> _searchDirectories = new();

        static void Main(string[] args)
        {
            var root = Directory.GetCurrentDirectory();

            var inputs = new[]
            {
                new { Dll = "src/LeichtFrame.Core/bin/Release/net8.0/LeichtFrame.Core.dll", Name = "LeichtFrame.Core" },
                new { Dll = "src/LeichtFrame.IO/bin/Release/net8.0/LeichtFrame.IO.dll", Name = "LeichtFrame.IO" }
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
            if (Directory.Exists(generatedRootDir))
            {
                Directory.Delete(generatedRootDir, true);
            }

            Console.WriteLine("🚀 Starting Pure .NET Doc Generator...");

            foreach (var input in inputs)
            {
                var dllPath = Path.Combine(root, input.Dll);
                var xmlPath = Path.ChangeExtension(dllPath, ".xml");

                if (!File.Exists(dllPath))
                {
                    Console.WriteLine($"❌ DLL missing: {dllPath}");
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
                        .OrderBy(t => t.Name)
                        .ToList();
                }
                catch (ReflectionTypeLoadException ex)
                {
                    Console.WriteLine($"⚠️ Warning: Some types in {input.Name} could not be loaded.");
                    types = ex.Types.Where(t => t != null && !t.Name.StartsWith("<") && !t.IsSpecialName)
                                    .OrderBy(t => t!.Name).Select(t => t!).ToList();
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
            var loaded = AppDomain.CurrentDomain.GetAssemblies().FirstOrDefault(a => a.GetName().Name == assemblyName);
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
                try { Assembly.LoadFrom(dll); } catch { }
            }
        }

        static void GenerateNamespaceIndex(string namespaceName, List<Type> types, string outputDir, Dictionary<string, string> docMap)
        {
            var sb = new StringBuilder();

            var shortName = namespaceName.Contains('.')
                ? namespaceName.Substring(namespaceName.LastIndexOf('.') + 1)
                : namespaceName;

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
                string summary = "";
                if (docMap.TryGetValue(typeId, out var rawSummary))
                {
                    summary = CleanDoc(rawSummary).Split('.')[0] + ".";
                }
                string typeNameSafe = t.Name.Replace("<", "&lt;").Replace(">", "&gt;");

                string safeFilename = GetSafeFilename(t);
                sb.AppendLine($"| [{typeNameSafe}](./{safeFilename}.md) | {ToMdxSafe(summary)} |");
            }

            File.WriteAllText(Path.Combine(outputDir, "index.md"), sb.ToString());
            Console.WriteLine($"   Generated Index: {namespaceName}/index.md (Label: {shortName})");
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
                sb.AppendLine(ToMdxSafe(summary.Trim()));
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
                    string doc = docMap.ContainsKey(propId) ? CleanDoc(docMap[propId]) : "";
                    string typeName = CleanTypeName(p.PropertyType);

                    sb.AppendLine($"| `{p.Name}` | `{ToMdxSafe(typeName)}` | {ToMdxSafe(doc)} |");
                }
                sb.AppendLine();
            }

            var methods = type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)
                .Where(m => !m.IsSpecialName
                            && !m.Name.StartsWith("<")
                            && m.Name != "GetType"
                            && m.Name != "ToString"
                            && m.Name != "Equals"
                            && m.Name != "GetHashCode"
                            && m.Name != "Deconstruct")
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

                        var parameters = m.GetParameters();
                        var paramStr = string.Join(", ", parameters.Select(p => $"{CleanTypeName(p.ParameterType)} {p.Name}"));
                        var returnType = CleanTypeName(m.ReturnType);

                        sb.AppendLine($"```csharp\npublic {returnType} {m.Name}({paramStr})\n```");

                        string methodId = GetMethodId(m);
                        if (docMap.TryGetValue(methodId, out var mDoc))
                        {
                            sb.AppendLine(ToMdxSafe(mDoc.Trim()));
                        }
                        sb.AppendLine();
                    }
                    catch (Exception)
                    {
                        sb.AppendLine($"*(Method documentation unavailable due to missing dependencies)*");
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
            return t.Name.Replace('`', '_');
        }

        static string ToMdxSafe(string text)
        {
            if (string.IsNullOrEmpty(text)) return text;
            return text.Replace("<", "&lt;").Replace(">", "&gt;");
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
                    var summary = member.Element("summary")?.Value;

                    if (name != null && summary != null)
                    {
                        map[name] = string.Join(" ", summary.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).Select(s => s.Trim()));
                    }
                }
            }
            catch { }
            return map;
        }

        static string GetMethodId(MethodInfo m)
        {
            var sb = new StringBuilder();
            sb.Append("M:");
            sb.Append(m.DeclaringType?.FullName);
            sb.Append(".");
            sb.Append(m.Name);

            var parameters = m.GetParameters();
            if (parameters.Length > 0)
            {
                sb.Append("(");
                for (int i = 0; i < parameters.Length; i++)
                {
                    if (i > 0) sb.Append(",");
                    sb.Append(parameters[i].ParameterType.FullName ?? parameters[i].ParameterType.Name);
                }
                sb.Append(")");
            }
            return sb.ToString();
        }

        static string CleanTypeName(Type t)
        {
            if (t == typeof(int)) return "int";
            if (t == typeof(string)) return "string";
            if (t == typeof(bool)) return "bool";
            if (t == typeof(double)) return "double";
            if (t == typeof(void)) return "void";

            if (t.IsGenericType)
            {
                var name = t.Name.Split('`')[0];
                var args = string.Join(", ", t.GetGenericArguments().Select(CleanTypeName));
                return $"{name}<{args}>";
            }
            return t.Name.Replace("&", "");
        }

        static string CleanDoc(string doc) => doc.Replace("\n", " ").Trim();
    }
}