from gitlint.rules import CommitRule, RuleViolation
import re
import difflib

PROJECTS = [
    'lib',
    'sdk',
    'validator',
    'miner',
    'executor',
    'facilitator',
]

class ImpactsProjects(CommitRule):
    """Enforce that each commit contains an "Impacts:" line with a list
    of defined projects.
    """

    name = "body-requires-impacts-footer"

    # A rule MUST have a *unique* id
    # We recommend starting with UC (for User-defined Commit-rule).
    id = "UR1"

    regex = re.compile(r'(?im)^\s*impacts:\s*(.*)$')

    def validate(self, commit):
        # Find all projects listed in any Impacts footer lines
        full_msg = commit.message.full or ""
        matches = self.regex.findall(full_msg)
        projects: list[str] = []
        for m in matches:
            for part in m.split(","):
                name = part.strip()
                if name:
                    projects.append(name)

        # If no Impacts header at all, or header without any project names
        header_present = bool(matches)
        if not header_present:
            msg = ("Commit message is missing an 'Impacts:' footer. "
                   "Add a line like: Impacts: validator, miner")
            return [RuleViolation(self.id, msg, line_nr=1)]

        if not projects:
            msg = ("'Impacts:' footer is present but no projects were listed. "
                   f"Allowed projects: {', '.join(PROJECTS)}")
            return [RuleViolation(self.id, msg, line_nr=1)]

        # Validate project names
        allowed = set(PROJECTS)
        invalid_parts: list[str] = []
        for p in projects:
            if p not in allowed:
                suggestion = difflib.get_close_matches(p, PROJECTS, n=1, cutoff=0.7)
                if suggestion:
                    invalid_parts.append(f"{p} (did you mean '{suggestion[0]}'?)")
                else:
                    invalid_parts.append(p)

        if invalid_parts:
            msg = ("Invalid project name(s) in 'Impacts:' footer: "
                   + ", ".join(invalid_parts)
                   + ". Allowed projects: " + ", ".join(PROJECTS))
            return [RuleViolation(self.id, msg, line_nr=1)]

        # All good
        return []