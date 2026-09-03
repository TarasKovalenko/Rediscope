# rediscope installer for Windows.
#
#   irm https://raw.githubusercontent.com/TarasKovalenko/Rediscope/main/install.ps1 | iex
#
# Environment:
#   REDISCOPE_VERSION   tag to install (default: latest release)
#   REDISCOPE_BIN_DIR   install directory (default: %LOCALAPPDATA%\Programs\rediscope\bin)
#   REDISCOPE_REPO      owner/name to install from (default: TarasKovalenko/Rediscope)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
# TLS 1.2 for Windows PowerShell 5.1, which still defaults to SSL3/TLS1.
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$repo = if ($env:REDISCOPE_REPO) { $env:REDISCOPE_REPO } else { 'TarasKovalenko/Rediscope' }
$bin  = 'rediscope'

function Die($msg) { Write-Host "error: $msg" -ForegroundColor Red; exit 1 }
function Dim($msg) { Write-Host "  $msg" -ForegroundColor DarkGray }

# ---- target detection ----------------------------------------------------
$arch = switch ($env:PROCESSOR_ARCHITECTURE) {
  'AMD64' { 'x86_64' }
  'ARM64' { 'aarch64' }
  'x86'   { if ([Environment]::Is64BitOperatingSystem) { 'x86_64' } else { $null } }
  default { $null }
}
if (-not $arch) { Die "unsupported architecture: $env:PROCESSOR_ARCHITECTURE" }
$target = "$arch-pc-windows-msvc"

# ---- version -------------------------------------------------------------
$version = $env:REDISCOPE_VERSION
if (-not $version) {
  try {
    $release = Invoke-RestMethod -UseBasicParsing `
      -Uri "https://api.github.com/repos/$repo/releases/latest" `
      -Headers @{ 'User-Agent' = 'rediscope-installer' }
    $version = $release.tag_name
  } catch {
    Die "could not determine the latest release of $repo ($_)"
  }
}
if (-not $version) { Die "could not determine the latest release of $repo" }

# ---- install directory ---------------------------------------------------
$binDir = if ($env:REDISCOPE_BIN_DIR) {
  $env:REDISCOPE_BIN_DIR
} else {
  Join-Path $env:LOCALAPPDATA 'Programs\rediscope\bin'
}

$asset = "$bin-$version-$target.zip"
$base  = "https://github.com/$repo/releases/download/$version"

Write-Host "Installing $bin $version ($target)" -ForegroundColor White
$tmp = Join-Path ([IO.Path]::GetTempPath()) ("rediscope-" + [Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $tmp -Force | Out-Null
try {
  Dim "downloading $asset"
  try {
    Invoke-WebRequest -UseBasicParsing -Uri "$base/$asset" -OutFile (Join-Path $tmp $asset)
  } catch {
    Die "no prebuilt binary for $target in release $version"
  }

  # ---- checksum ----------------------------------------------------------
  $sums = Join-Path $tmp 'SHA256SUMS'
  try {
    Invoke-WebRequest -UseBasicParsing -Uri "$base/SHA256SUMS" -OutFile $sums
  } catch {
    $sums = $null
    Dim 'skipping checksum (SHA256SUMS not published for this release)'
  }
  if ($sums) {
    $line = Select-String -Path $sums -Pattern ([Regex]::Escape($asset) + '$') |
      Select-Object -First 1
    if ($line) {
      $expected = ($line.Line -split '\s+')[0]
      $actual = (Get-FileHash -Algorithm SHA256 -Path (Join-Path $tmp $asset)).Hash
      if ($actual -ne $expected.ToUpper()) {
        Die "checksum mismatch for $asset (expected $expected, got $actual)"
      }
      Dim 'checksum verified'
    }
  }

  Expand-Archive -Path (Join-Path $tmp $asset) -DestinationPath $tmp -Force
  $exe = Join-Path $tmp "$bin.exe"
  if (-not (Test-Path $exe)) { Die "archive did not contain a '$bin.exe'" }

  New-Item -ItemType Directory -Path $binDir -Force | Out-Null
  $dest = Join-Path $binDir "$bin.exe"
  try {
    Move-Item -Path $exe -Destination $dest -Force
  } catch {
    Die "could not write $dest — close any running $bin and try again"
  }
} finally {
  Remove-Item -Recurse -Force $tmp -ErrorAction SilentlyContinue
}

Write-Host "Installed $dest" -ForegroundColor White

# ---- PATH ----------------------------------------------------------------
$userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
$onPath = ($userPath -split ';' | Where-Object { $_.TrimEnd('\') -ieq $binDir.TrimEnd('\') })
if ($onPath) {
  Dim "run: $bin"
} else {
  $joined = if ([string]::IsNullOrEmpty($userPath)) { $binDir } else { "$userPath;$binDir" }
  [Environment]::SetEnvironmentVariable('Path', $joined, 'User')
  $env:Path = "$env:Path;$binDir"
  Dim "added $binDir to your user PATH (open a new terminal for it to take effect)"
}
