param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet('prepare', 'generate', 'inspect', 'demo')]
    [string]$Command,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$CommandArguments
)
$ErrorActionPreference = 'Stop'
Push-Location (Split-Path $PSScriptRoot -Parent)
try {
    $mainClasses = @{
        prepare = 'io.github.jayhuangsci.corpus.hadoop.CorpusPipeline'
        generate = 'io.github.jayhuangsci.corpus.tools.GenerateCorpus'
        inspect = 'io.github.jayhuangsci.corpus.tools.CorpusInspector'
        demo = 'io.github.jayhuangsci.corpus.tools.Demo'
    }
    if (!(Test-Path target/classpath.txt) -or !(Test-Path target/classes/io/github/jayhuangsci/corpus/hadoop/CorpusPipeline.class)) {
        & .\mvnw.cmd -q '-DskipTests' package
        if ($LASTEXITCODE -ne 0) { throw 'Maven build failed' }
    }
    $java = if ($env:JAVA_HOME) { Join-Path $env:JAVA_HOME 'bin/java.exe' } else { 'java' }
    $classpath = (Join-Path (Get-Location).Path 'target/classes') + [IO.Path]::PathSeparator + (Get-Content target/classpath.txt -Raw).Trim()
    $argumentFile = Join-Path (Get-Location).Path 'target/java-classpath.args'
    [IO.File]::WriteAllText($argumentFile, ('-cp' + "`n" + '"' + $classpath.Replace('\', '/') + '"'), (New-Object System.Text.UTF8Encoding($false)))
    & $java -Xmx1536m '-Dorg.slf4j.simpleLogger.defaultLogLevel=warn' ("@" + $argumentFile) $mainClasses[$Command] @CommandArguments
    if ($LASTEXITCODE -ne 0) { throw "Command failed with exit code $LASTEXITCODE" }
} finally { Pop-Location }
