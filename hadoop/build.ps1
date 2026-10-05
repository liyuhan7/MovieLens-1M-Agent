param(
    [switch]$VerboseLog
)
$ErrorActionPreference = "Stop"
$hadoop = $PSScriptRoot                              # ml-1m\hadoop
$jdk8 = $env:ML_JAVA_HOME
if (-not $jdk8) {
    $jdk8 = @(
        "${env:ProgramFiles}\Eclipse Adoptium\jdk-8*",
        "${env:ProgramFiles}\Java\jdk1.8*",
        "${env:ProgramFiles(x86)}\Eclipse Adoptium\jdk-8*",
        "$env:LOCALAPPDATA\Programs\Eclipse Adoptium\jdk-8*"
    ) | ForEach-Object { Get-Item -Path $_ -ErrorAction SilentlyContinue } |
        Select-Object -First 1 -ExpandProperty FullName
}
if (-not $jdk8) { throw "未找到 JDK 8;请设置 ML_JAVA_HOME 指向 JDK 8 安装目录" }
$build = Join-Path $hadoop "build"
$classes = Join-Path $build "classes"
$src = Join-Path $hadoop "src\main\java\mliter1\IterationOne.java"
$depsDir = Join-Path $build "deps"
if (-not (Test-Path $depsDir)) {
    throw "缺少依赖 jar 目录: $depsDir(提取方法见 README.md「准备 Hadoop 依赖 jar」)"
}
$jars = @(Get-ChildItem (Join-Path $depsDir "*.jar"))
if ($jars.Count -eq 0) {
    throw "依赖 jar 为空: $depsDir(提取方法见 README.md「准备 Hadoop 依赖 jar」)"
}
$deps = ($jars | ForEach-Object FullName) -join ";"
New-Item -ItemType Directory -Force $classes | Out-Null
$resolvedBuild = [System.IO.Path]::GetFullPath($build)
$resolvedClasses = [System.IO.Path]::GetFullPath($classes)
if (-not $resolvedClasses.StartsWith($resolvedBuild + [System.IO.Path]::DirectorySeparatorChar, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "Refusing cleanup outside Hadoop build directory"
}
Get-ChildItem -LiteralPath $resolvedClasses -Force | ForEach-Object { Remove-Item -LiteralPath $_.FullName -Recurse -Force }
& "$jdk8\bin\javac.exe" -encoding UTF-8 -source 8 -target 8 -cp $deps -d $classes $src
if ($LASTEXITCODE -ne 0) { throw "javac failed" }
$jar = Join-Path $build "iter1.jar"
Remove-Item $jar -Force -ErrorAction SilentlyContinue
& "$jdk8\bin\jar.exe" -cf $jar -C $classes .
Write-Host "BUILD OK -> $jar"
