param(
    [switch]$VerboseLog
)
$ErrorActionPreference = "Stop"
$hadoop = $PSScriptRoot                              # ml-1m\hadoop
$jdk8 = "C:\Program Files\Eclipse Adoptium\jdk-8.0.452.9-hotspot"
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
Remove-Item (Join-Path $classes "*") -Recurse -Force -ErrorAction SilentlyContinue
& "$jdk8\bin\javac.exe" -encoding UTF-8 -source 8 -target 8 -cp $deps -d $classes $src
if ($LASTEXITCODE -ne 0) { throw "javac failed" }
$jar = Join-Path $build "iter1.jar"
Remove-Item $jar -Force -ErrorAction SilentlyContinue
& "$jdk8\bin\jar.exe" -cf $jar -C $classes .
Write-Host "BUILD OK -> $jar"
