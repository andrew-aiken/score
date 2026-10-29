package agent

import "testing"

func TestInboxPrefix(t *testing.T) {
	tests := []struct {
		name  string
		teams []uint16
		want  string
	}{
		{name: "SingleTeam", teams: []uint16{3}, want: "_AGENT.3"},
		{name: "MultipleTeams", teams: []uint16{0, 1, 2}, want: "_AGENT.0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := inboxPrefix(tt.teams); got != tt.want {
				t.Errorf("inboxPrefix(%v) = %q, want %q", tt.teams, got, tt.want)
			}
		})
	}
}
