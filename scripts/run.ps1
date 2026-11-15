param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet('hadoop', 'spark', 'generate', 'compare', 'demo')]
    [string]$Command,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$CommandArguments
)
$ErrorActionPreference = 'Stop'
Push-Location (Split-Path $PSScriptRoot -Parent)
try {
    $mainClasses = @{
        hadoop = 'io.github.jayhuangsci.ratings.hadoop.HadoopAnalytics'
        spark = 'io.github.jayhuangsci.ratings.spark.SparkAnalytics'
        generate = 'io.github.jayhuangsci.ratings.tools.GenerateRatings'
        compare = 'io.github.jayhuangsci.ratings.tools.CompareOutputs'
        demo = 'io.github.jayhuangsci.ratings.tools.Demo'
    }
    if (!(Test-Path target/classpath.txt) -or !(Test-Path target/classes/io/github/jayhuangsci/ratings/tools/Demo.class)) {
        & .\mvnw.cmd -q '-DskipTests' package
        if ($LASTEXITCODE -ne 0) { throw 'Maven build failed' }
    }
    $java = if ($env:JAVA_HOME) { Join-Path $env:JAVA_HOME 'bin/java.exe' } else { 'java' }
    $classpath = (Join-Path (Get-Location).Path 'target/classes') + [IO.Path]::PathSeparator + (Get-Content target/classpath.txt -Raw).Trim()
    # An argument file avoids Windows command length limits with Spark's dependency tree.
    $argumentFile = Join-Path (Get-Location).Path 'target/java-classpath.args'
    $utf8 = New-Object System.Text.UTF8Encoding($false)
    [IO.File]::WriteAllText($argumentFile, ('-cp' + "`n" + '"' + $classpath.Replace('\', '/') + '"'), $utf8)
    if (!$env:SPARK_LOCAL_IP) { $env:SPARK_LOCAL_IP = '127.0.0.1' }
    & $java -Xmx2g '--add-opens=java.base/java.lang=ALL-UNNAMED' '--add-opens=java.base/java.nio=ALL-UNNAMED' '--add-opens=java.base/java.util=ALL-UNNAMED' '--add-opens=java.base/sun.nio.ch=ALL-UNNAMED' ("@" + $argumentFile) $mainClasses[$Command] @CommandArguments
    if ($LASTEXITCODE -ne 0) { throw "Command failed with exit code $LASTEXITCODE" }
} finally { Pop-Location }
