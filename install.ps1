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

  # ---- checksum and signed provenance ------------------------------------
  $sums = Join-Path $tmp 'SHA256SUMS'
  try {
    Invoke-WebRequest -UseBasicParsing -Uri "$base/SHA256SUMS" -OutFile $sums
  } catch { Die 'SHA256SUMS is required' }
  $lines = @(Select-String -Path $sums -Pattern ('^[a-fA-F0-9]{64}\s+' + [Regex]::Escape($asset) + '$'))
  if ($lines.Count -ne 1) { Die "missing or ambiguous checksum for $asset" }
  $expected = ($lines[0].Line -split '\s+')[0]
  $actual = (Get-FileHash -Algorithm SHA256 -Path (Join-Path $tmp $asset)).Hash
  if ($actual -ne $expected.ToUpper()) { Die "checksum mismatch for $asset" }
  Dim 'checksum verified'
  # Verifying needs the GitHub CLI, and only releases built after provenance was
  # introduced carry an attestation to check. 'auto' tells three cases apart: a
  # signed release (verify, and refuse anything that does not match), an unsigned
  # one (say so, and ask), and a missing gh (say so, and ask). A failed check on
  # a release that *is* signed is fatal in every mode.
  $verify = if ($env:REDISCOPE_VERIFY_PROVENANCE) { $env:REDISCOPE_VERIFY_PROVENANCE } else { 'auto' }
  if ($verify -notin @('auto', '1', '0')) { Die 'REDISCOPE_VERIFY_PROVENANCE must be auto, 1 or 0' }

  function Invoke-Provenance {
    $out = & gh attestation verify (Join-Path $tmp $asset) --repo $repo `
      --signer-workflow "$repo/.github/workflows/release.yml" `
      --source-ref "refs/tags/$version" --deny-self-hosted-runners 2>&1
    [pscustomobject]@{ Ok = ($LASTEXITCODE -eq 0); Output = ($out | Out-String) }
  }
  # An unsigned release answers the attestations endpoint with HTTP 404.
  function Test-Unsigned($text) { $text -match 'HTTP 404|no attestations found' }
  function Test-Unreachable($text) {
    $text -match 'gh auth login|authentication|not logged|HTTP 401|HTTP 403|no such host|connection refused|timed? out'
  }
  function Confirm-Unverified($reason) {
    Write-Host "warning: $reason" -ForegroundColor Red
    Write-Host 'warning: only the SHA-256 checksum vouches for this download' -ForegroundColor Red
    # iwr | iex leaves no usable stdin, so ask on the console itself.
    if ([Environment]::UserInteractive -and -not [Console]::IsInputRedirected) {
      $answer = Read-Host "Install $bin $version without verified build provenance? [y/N]"
      if ($answer -notmatch '^(y|yes)$') { Die 'cancelled' }
      Dim 'continuing without provenance verification'
    } else {
      Write-Host 'warning: no console to ask on; continuing with the checksum alone' -ForegroundColor Red
      Dim 'set REDISCOPE_VERIFY_PROVENANCE=1 to make this fatal instead'
    }
  }

  if ($verify -eq '0') {
    Dim 'WARNING: provenance verification explicitly disabled'
  } elseif (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
    if ($verify -eq '1') { Die 'GitHub CLI (gh) is required for provenance verification' }
    Confirm-Unverified 'cannot verify build provenance: the GitHub CLI (gh) is not installed'
  } else {
    $result = Invoke-Provenance
    if ($result.Ok) {
      Dim 'signed build provenance verified'
    } elseif ($verify -eq 'auto' -and (Test-Unsigned $result.Output)) {
      Confirm-Unverified "$version was published before rediscope signed its releases"
    } elseif ($verify -eq 'auto' -and (Test-Unreachable $result.Output)) {
      Confirm-Unverified "cannot reach GitHub to check this release's build provenance"
    } else {
      Write-Host $result.Output.Trim() -ForegroundColor Red
      Die 'release provenance verification failed; nothing installed'
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
