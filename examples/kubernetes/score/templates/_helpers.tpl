{{/*
Chart name, truncated and DNS1123-safe.
*/}}
{{- define "score.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Fully qualified app name.
*/}}
{{- define "score.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Common labels.
*/}}
{{- define "score.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.global.labels }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/*
Selector labels for a given component (e.g. "server", "agent", "setup").
*/}}
{{- define "score.selectorLabels" -}}
app.kubernetes.io/name: {{ include "score.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/*
NATS connection address used by server/agent.
*/}}
{{- define "score.natsAddress" -}}
{{- if .Values.natsAddress -}}
{{ .Values.natsAddress }}
{{- else -}}
nats://{{ .Release.Name }}-nats:4222
{{- end -}}
{{- end -}}

{{/*
Public-facing NATS websocket URL
*/}}
{{- define "score.natsPublicUrl" -}}
{{- if .Values.natsPublicUrl -}}
{{ .Values.natsPublicUrl }}
{{- else if .Values.natsWebsocketIngress.enabled -}}
wss://{{ .Values.natsWebsocketIngress.host }}
{{- end -}}
{{- end -}}

{{/*
Secret name for the server.creds bundle
*/}}
{{- define "score.serverCredsSecretName" -}}
{{- .Values.serverCreds.existingSecret | default .Values.serverCreds.secretName -}}
{{- end -}}

{{/*
Secret name for config.json
*/}}
{{- define "score.configSecretName" -}}
{{- .Values.config.existingSecret | default .Values.config.secretName -}}
{{- end -}}

{{/*
Secret name for the default agent creds bundle
*/}}
{{- define "score.agentCredsSecretName" -}}
{{- .Values.agentCreds.existingSecret | default .Values.agentCreds.secretName -}}
{{- end -}}
