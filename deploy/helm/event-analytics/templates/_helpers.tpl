{{- define "ea.name" -}}event-analytics{{- end -}}

{{- define "ea.labels" -}}
app.kubernetes.io/name: {{ include "ea.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "ea.selector" -}}
app.kubernetes.io/name: {{ include "ea.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "ea.secretName" -}}
{{- default (printf "%s-secrets" .Release.Name) .Values.existingSecret -}}
{{- end -}}

{{- define "ea.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag }}
{{- end -}}

{{/*
Pod-level defaults for every app pod:
- enableServiceLinks: false. Kubernetes otherwise injects legacy
  `<SERVICE>_PORT=tcp://ip:port` env vars for every Service in the namespace.
  The `clickhouse` Service yields CLICKHOUSE_PORT=tcp://10.96.x.x:8123, which
  collides with our CLICKHOUSE_PORT setting and crashed startup. We use DNS
  names, so the links are pure noise.
- Restricted security: non-root, read-only root filesystem, no capabilities.
*/}}
{{- define "ea.podSecurity" -}}
enableServiceLinks: false
securityContext:
  runAsNonRoot: true
  runAsUser: 1000
  runAsGroup: 1000
  fsGroup: 1000
  seccompProfile:
    type: RuntimeDefault
{{- end -}}

{{- define "ea.containerSecurity" -}}
securityContext:
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  capabilities:
    drop: ["ALL"]
{{- end -}}

{{- define "ea.envFrom" -}}
envFrom:
  - configMapRef:
      name: {{ .Release.Name }}-config
  - secretRef:
      name: {{ include "ea.secretName" . }}
{{- end -}}
