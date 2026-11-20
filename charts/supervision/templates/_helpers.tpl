{{/* Convertit une durée Prometheus simple (« 15m », « 2h », « 30s ») en secondes. */}}
{{- define "supervision.secondes" -}}
{{- $d := toString . -}}
{{- if hasSuffix "h" $d -}}{{ mul (trimSuffix "h" $d | atoi) 3600 }}
{{- else if hasSuffix "m" $d -}}{{ mul (trimSuffix "m" $d | atoi) 60 }}
{{- else -}}{{ trimSuffix "s" $d | atoi }}
{{- end -}}
{{- end -}}
