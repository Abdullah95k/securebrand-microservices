{{- define "fbs.fullname" -}}
{{- printf "%s" .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- end -}}

{{- define "fbs.labels" -}}
app.kubernetes.io/name: fbs
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "fbs.selector" -}}
app.kubernetes.io/name: fbs
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .name }}
{{- end -}}

{{/* "true" when the pod or hook named .name mounts secretFiles: those in
secretFiles.components, or every one when the list is empty. Takes
(dict "root" $root "name" $name), like the pieces below. */}}
{{- define "fbs.secretFilesOn" -}}
{{- $sf := .root.Values.secretFiles -}}
{{- if and $sf.existingSecret (or (empty $sf.components) (has .name $sf.components)) -}}true{{- end -}}
{{- end -}}

{{- define "fbs.configFiles" -}}
{{- if include "fbs.secretFilesOn" . -}}
/etc/fbs/config.yaml,/etc/fbs-secret/{{ .root.Values.secretFiles.key }}
{{- else -}}
/etc/fbs/config.yaml
{{- end -}}
{{- end -}}

{{/* Shared pod spec pieces: env, volumes, mounts. Each takes
(dict "root" $root "name" <component or hook>). */}}
{{- define "fbs.env" -}}
- name: FBS_CONFIG
  value: {{ include "fbs.configFiles" . | quote }}
{{- if .root.Values.platformCredentials.existingSecret }}
- name: FBS_PLATFORM__CREDENTIALS_DIR
  value: {{ .root.Values.platformCredentials.mountPath | quote }}
{{- end }}
{{- end -}}

{{- define "fbs.envFrom" -}}
{{- if .Values.secretEnv.existingSecret }}
envFrom:
  - secretRef:
      name: {{ .Values.secretEnv.existingSecret }}
{{- end }}
{{- end -}}

{{- define "fbs.volumeMounts" -}}
{{- $root := .root -}}
- name: config
  mountPath: /etc/fbs
  readOnly: true
{{- if include "fbs.secretFilesOn" . }}
- name: secret-files
  mountPath: /etc/fbs-secret
  readOnly: true
{{- end }}
{{- if $root.Values.tlsFiles.existingSecret }}
- name: tls-files
  mountPath: {{ $root.Values.tlsFiles.mountPath }}
  readOnly: true
{{- end }}
{{- if $root.Values.platformCredentials.existingSecret }}
- name: platform-credentials
  mountPath: {{ $root.Values.platformCredentials.mountPath }}
  readOnly: true
{{- end }}
- name: tmp
  mountPath: /tmp
{{- end -}}

{{- define "fbs.volumes" -}}
{{- include "fbs.volumesFor" (merge (dict "configMap" (printf "%s-config" (include "fbs.fullname" .root))) .) -}}
{{- end -}}

{{- define "fbs.hookVolumes" -}}
{{- include "fbs.volumesFor" (merge (dict "configMap" (printf "%s-hook-config" (include "fbs.fullname" .root))) .) -}}
{{- end -}}

{{- define "fbs.volumesFor" -}}
{{- $root := .root -}}
- name: config
  configMap:
    name: {{ .configMap }}
{{- if include "fbs.secretFilesOn" . }}
- name: secret-files
  secret:
    secretName: {{ $root.Values.secretFiles.existingSecret }}
{{- end }}
{{- if $root.Values.tlsFiles.existingSecret }}
- name: tls-files
  secret:
    secretName: {{ $root.Values.tlsFiles.existingSecret }}
{{- end }}
{{- if $root.Values.platformCredentials.existingSecret }}
- name: platform-credentials
  secret:
    secretName: {{ $root.Values.platformCredentials.existingSecret }}
{{- end }}
- name: tmp
  emptyDir: {}
{{- end -}}
