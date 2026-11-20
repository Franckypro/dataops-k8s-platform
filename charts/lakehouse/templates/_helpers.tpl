{{- define "lakehouse.labels" -}}
app.kubernetes.io/part-of: dataops-platform
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "lakehouse.image" -}}
{{- if .digest -}}{{ .repository }}@{{ .digest }}{{- else -}}{{ .repository }}:{{ .tag }}{{- end -}}
{{- end -}}

{{- define "lakehouse.minioEndpoint" -}}
http://minio.{{ .Release.Namespace }}.svc.cluster.local:9000
{{- end -}}
