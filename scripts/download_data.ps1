param(
    [Parameter(Mandatory = $false)]
    [string]$OutDir = "data/raw"
)

$ErrorActionPreference = "Stop"

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

Write-Host "Baixando schema.yml"
Invoke-WebRequest -Uri "https://storage.googleapis.com/case_vagas/whatsapp/schema.yml" -OutFile (Join-Path $OutDir "schema.yml")

Write-Host "Tentando copiar dados via gcloud storage cp"
try {
    $null = gcloud --version
    gcloud storage cp -r "gs://case_vagas/whatsapp/*" $OutDir
    if ($LASTEXITCODE -ne 0) {
        throw "Falha ao copiar do bucket. Execute gcloud auth login e tente novamente."
    }
    Write-Host "Copia concluida via gcloud"
}
catch {
    Write-Host "Falha ao copiar via gcloud."
    Write-Host "Passo necessario para autenticar"
    Write-Host "gcloud auth login"
    Write-Host "Se voce nao tem o gcloud instalado, instale pelo winget"
    Write-Host "winget install -e --id Google.CloudSDK"
    exit 1
}

