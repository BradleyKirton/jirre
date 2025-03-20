import typing as t
import os
import rich
from rich.table import Table
import datetime
import sqlite3
import dataclasses
import argparse
import configparser
import pathlib


class JireError(Exception): ...


class ConfigError(JireError): ...


def adapt_datetime_epoch(val: datetime.datetime) -> int:
    """Adapt datetime.datetime to Unix timestamp."""
    return int(val.timestamp())


sqlite3.register_adapter(datetime.datetime, adapt_datetime_epoch)


RETURNING_CLAUSE = """
RETURNING
    rowid,
    name,
    description,
    project,
    status,
    assigned_to,
    notes,
    created_by,
    updated_by,
    created_at,
    updated_at;
"""


@dataclasses.dataclass
class Ticket:
    """Models a ticket."""

    rowid: int
    name: str
    description: str
    project: str
    status: str
    assigned_to: str | None
    notes: str
    created_by: str
    updated_by: str
    created_at: datetime.datetime
    updated_at: datetime.datetime | None

    # Status
    TODO = "TODO"
    DOING = "DOING"
    DONE = "DONE"

    @classmethod
    def get(cls, conn: sqlite3.Connection, rowid: int) -> t.Self:
        """Get a ticket by rowid."""
        sql = """
        SELECT
            rowid,
            name,
            description,
            project,
            status,
            assigned_to,
            notes,
            created_by,
            updated_by,
            created_at,
            updated_at
        FROM ticket
        WHERE rowid = :rowid
        """
        params = {
            "rowid": rowid,
        }
        with conn:
            conn.row_factory = cls.row_factory
            cursor = conn.execute(sql, params)
            ticket = cursor.fetchone()
        return ticket

    @classmethod
    def row_factory(cls, cursor: sqlite3.Cursor, row: tuple[t.Any, ...]) -> t.Self:
        """Sqlite row factory for the Ticket class."""
        headers = [v[0] for v in cursor.description]
        raw_data = dict(zip(headers, row))
        rowid = raw_data["rowid"]
        name = raw_data["name"]
        description = raw_data["description"]
        project = raw_data["project"]
        status = raw_data["status"]
        assigned_to = raw_data["assigned_to"]
        notes = raw_data["notes"]
        created_by = raw_data["created_by"]
        created_at = raw_data["created_at"]
        updated_by = raw_data["updated_by"]
        updated_at = raw_data["updated_at"]
        created_at = datetime.datetime.fromtimestamp(created_at)

        if updated_at:
            updated_at = datetime.datetime.fromtimestamp(updated_at)

        return cls(
            rowid=rowid,
            name=name,
            description=description,
            project=project,
            status=status,
            assigned_to=assigned_to,
            notes=notes,
            created_by=created_by,
            updated_by=updated_by,
            created_at=created_at,
            updated_at=updated_at,
        )

    @classmethod
    def new(
        cls,
        conn: sqlite3.Connection,
        name: str,
        description: str,
        project: str,
        assigned_to: str | None,
        created_by: str,
        current_datetime: datetime.datetime,
    ) -> t.Self:
        """Create a new ticket."""
        sql = f"""
        INSERT INTO ticket (name, description, project, status, assigned_to, created_by, created_at)
        VALUES (:name, :description, :project, :status, :assigned_to, :created_by, :created_at)
        {RETURNING_CLAUSE}
        """
        params = {
            "name": name,
            "status": Ticket.TODO,
            "description": description,
            "project": project,
            "assigned_to": assigned_to,
            "created_by": created_by,
            "created_at": current_datetime,
        }
        with conn:
            conn.row_factory = cls.row_factory
            cursor = conn.execute(sql, params)
            ticket = cursor.fetchone()
        return ticket

    def asdict(self) -> dict[str, str]:
        """Serialize the ticket to a dict."""
        if self.assigned_to:
            assigned_to = self.assigned_to
        else:
            assigned_to = ""

        if self.updated_at:
            updated_at = self.updated_at.isoformat()
        else:
            updated_at = ""

        if self.project:
            project = self.project
        else:
            project = ""

        return {
            "rowid": f"{self.rowid}",
            "name": self.name,
            "description": self.description,
            "project": project,
            "status": self.status,
            "assigned_to": assigned_to,
            "notes": self.notes,
            "created_by": self.created_by,
            "updated_by": self.updated_by,
            "created_at": self.created_at.isoformat(),
            "updated_at": updated_at,
        }

    def add_to_table(self, table: Table) -> None:
        """Add the ticket to a rich Table."""
        rowid = f"{self.rowid}"
        name = f"{self.name}"
        description = f"{self.description}"
        project = f"{self.project}"
        status = f"{self.status}"
        created_by = f"{self.created_by}"
        created_at = f"{self.created_at.isoformat()}"

        if self.notes:
            notes = f"{self.notes}"
        else:
            notes = ""

        if self.assigned_to:
            assigned_to = f"{self.assigned_to}"
        else:
            assigned_to = ""

        if self.updated_by:
            updated_by = f"{self.updated_by}"
        else:
            updated_by = ""

        if self.updated_at:
            updated_at = f"{self.updated_at.isoformat()}"
        else:
            updated_at = ""

        table.add_row(
            rowid,
            name,
            description,
            project,
            status,
            assigned_to,
            notes,
            created_by,
            created_at,
            updated_by,
            updated_at,
        )

    def assign_to(
        self,
        conn: sqlite3.Connection,
        status: str,
        assigned_to: str | None,
        updated_by: str,
        current_datetime: datetime.datetime,
    ) -> t.Self:
        """Assign the the ticket to a user."""
        sql = f"""
        UPDATE ticket SET
            status = :status,
            assigned_to = :assigned_to,
            updated_by = :updated_by,
            updated_at = :updated_at
        WHERE rowid = :rowid
        {RETURNING_CLAUSE}
        """
        params = {
            "rowid": self.rowid,
            "status": status,
            "assigned_to": assigned_to,
            "updated_by": updated_by,
            "updated_at": current_datetime,
        }
        with conn:
            conn.row_factory = self.row_factory
            cursor = conn.execute(sql, params)
            ticket = cursor.fetchone()
        return ticket

    def mark_as_done(
        self,
        conn: sqlite3.Connection,
        notes: str,
        updated_by: str,
        current_datetime: datetime.datetime,
    ) -> t.Self:
        """Mark the ticket as done."""
        sql = f"""
        UPDATE ticket SET
            status = :status,
            notes = :notes,
            assigned_to = NULL,
            updated_by = :updated_by,
            updated_at = :updated_at
        WHERE rowid = :rowid
        {RETURNING_CLAUSE}
        """
        params = {
            "rowid": self.rowid,
            "notes": notes,
            "status": Ticket.DONE,
            "updated_by": updated_by,
            "updated_at": current_datetime,
        }
        with conn:
            conn.row_factory = self.row_factory
            cursor = conn.execute(sql, params)
            ticket = cursor.fetchone()
        return ticket


def setup_db(conn: sqlite3.Connection) -> None:
    """Setup the database."""
    sql = """
    CREATE TABLE IF NOT EXISTS ticket (
        name text,
        description text,
        project text,
        status text NOT NULL,
        assigned_to text NULL,
        notes text DEFAULT '',
        created_by text NOT NULL,
        created_at timestamp NOT NULL,
        updated_by text NULL,
        updated_at timestamp NULL
    );
    """
    with conn:
        conn.execute(sql)

    sql = """
    CREATE VIRTUAL TABLE IF NOT EXISTS ticket_fts USING fts5(
        name,
        description,
        project,
        status,
        assigned_to,
        created_by,
        tokenize = 'porter ascii',
        content=ticket,
        content_rowid=rowid
    );
    """
    with conn:
        conn.execute(sql)

    sql = """
    CREATE TRIGGER IF NOT EXISTS insert_ticket AFTER INSERT ON ticket
    BEGIN
        INSERT INTO ticket_fts (
            rowid,
            name,
            description,
            project,
            status,
            assigned_to,
            created_by   
        )
        VALUES (
            NEW.rowid,
            NEW.name,
            NEW.description,
            NEW.project,
            NEW.status,
            NEW.assigned_to,
            NEW.created_by
        );
    END;
    """
    with conn:
        conn.execute(sql)

    sql = """
    CREATE TRIGGER IF NOT EXISTS update_ticket AFTER UPDATE ON ticket
    BEGIN
        DELETE FROM ticket_fts WHERE rowid = NEW.rowid;
        INSERT INTO ticket_fts (
            rowid,
            name,
            description,
            project,
            status,
            assigned_to,
            created_by   
        )
        VALUES (
            NEW.rowid,
            NEW.name,
            NEW.description,
            NEW.project,
            NEW.status,
            NEW.assigned_to,
            NEW.created_by
        );
    END;
    """
    with conn:
        conn.execute(sql)


def list_ticket_handler(
    args: argparse.Namespace,
    conn: sqlite3.Connection,
    user: str,
    current_datetime: datetime.datetime,
    config: dict[str, str],
) -> None:
    """List the available tickets according to the search criteria."""
    status = args.status
    search = args.search
    assigned_to = args.assigned_to
    created_by = args.created_by
    format = args.format

    _list_tickets(
        conn=conn,
        status=status,
        search=search,
        assigned_to=assigned_to,
        created_by=created_by,
        format=format,
    )


def _list_tickets(
    conn: sqlite3.Connection,
    status: str,
    search: str,
    assigned_to: str,
    created_by: str,
    format: str,
) -> None:
    """Print the lsit of tickets."""
    params = {}
    sql = """
    SELECT
        t.rowid,
        t.name,
        t.description,
        t.project,
        t.status,
        t.assigned_to,
        t.notes,
        t.created_by,
        t.created_at,
        t.updated_by,
        t.updated_at
    FROM ticket t
    """

    if search:
        params["search"] = search

        sql += """
            INNER JOIN ticket_fts ON ticket_fts.rowid  = t.rowid
                AND ticket_fts MATCH :search
        """

    sql += "WHERE 1 = 1"

    if status:
        sql += "    AND t.status = :status"
        params["status"] = status

    if assigned_to:
        sql += "    AND t.assigned_to = :assigned_to"
        params["assigned_to"] = assigned_to

    if created_by:
        sql += "    AND t.created_by = :created_by"
        params["created_by"] = created_by

    with conn:
        conn.row_factory = Ticket.row_factory
        cursor = conn.execute(sql, params)
        tickets = cursor.fetchall()

    if format == "json":
        rich.print_json(data=[ticket.asdict() for ticket in tickets])
    else:
        table = Table(title="Tickets")
        table.add_column("rowid")
        table.add_column("name")
        table.add_column("description")
        table.add_column("project")
        table.add_column("status")
        table.add_column("assigned_to")
        table.add_column("notes")
        table.add_column("created_by")
        table.add_column("created_at")
        table.add_column("updated_by")
        table.add_column("updated_at")

        for ticket in tickets:
            ticket.add_to_table(table=table)

        rich.print(table)


def create_ticket_handler(
    args: argparse.Namespace,
    conn: sqlite3.Connection,
    user: str,
    current_datetime: datetime.datetime,
    config: dict[str, str],
) -> None:
    """Create a new ticket."""
    format = args.format
    name = args.name
    description = args.description
    project = args.project or config.get("project", "default")

    if args.assign_to:
        assigned_to = args.assign_to
    else:
        assigned_to = user

    Ticket.new(
        conn=conn,
        name=name,
        description=description,
        project=project,
        assigned_to=assigned_to,
        created_by=user,
        current_datetime=current_datetime,
    )
    _list_tickets(
        conn=conn,
        status="",
        search="",
        assigned_to="",
        created_by="",
        format=format,
    )


def assign_todo_handler(
    args: argparse.Namespace,
    conn: sqlite3.Connection,
    user: str,
    current_datetime: datetime.datetime,
    config: dict[str, str],
) -> None:
    """Assign the ticket as todo."""
    format = args.format
    rowid = args.rowid

    if args.assign_to:
        assigned_to = args.assign_to
    else:
        assigned_to = None

    ticket = Ticket.get(conn=conn, rowid=rowid)
    ticket.assign_to(
        conn=conn,
        status=Ticket.TODO,
        assigned_to=assigned_to,
        updated_by=user,
        current_datetime=current_datetime,
    )
    _list_tickets(
        conn=conn,
        status="",
        search="",
        assigned_to="",
        created_by="",
        format=format,
    )


def assign_doing_handler(
    args: argparse.Namespace,
    conn: sqlite3.Connection,
    user: str,
    current_datetime: datetime.datetime,
    config: dict[str, str],
) -> None:
    """Assign the ticket as doing."""
    format = args.format
    rowid = args.rowid
    assigned_to = args.assign_to or user

    ticket = Ticket.get(conn=conn, rowid=rowid)
    ticket.assign_to(
        conn=conn,
        status=Ticket.DOING,
        assigned_to=assigned_to,
        updated_by=user,
        current_datetime=current_datetime,
    )
    _list_tickets(
        conn=conn,
        status="",
        search="",
        assigned_to="",
        created_by="",
        format=format,
    )


def mark_as_done_handler(
    args: argparse.Namespace,
    conn: sqlite3.Connection,
    user: str,
    current_datetime: datetime.datetime,
    config: dict[str, str],
) -> None:
    """Mark the ticket as DONE."""
    format = args.format
    rowid = args.rowid
    notes = args.notes

    ticket = Ticket.get(conn=conn, rowid=rowid)
    ticket.mark_as_done(
        conn=conn,
        notes=notes,
        updated_by=user,
        current_datetime=current_datetime,
    )
    _list_tickets(
        conn=conn,
        status="",
        search="",
        assigned_to="",
        created_by="",
        format=format,
    )


def sync_handler(
    args: argparse.Namespace,
    conn: sqlite3.Connection,
    user: str,
    current_datetime: datetime.datetime,
    config: dict[str, str],
) -> None:
    rich.print("[yellow]A sync feature may be cool for teams")


def find_config_path(root_path: pathlib.Path) -> pathlib.Path:
    """Traverse upwards for an override project path."""
    config_path = next(root_path.glob(".jire.config"), None)

    if config_path:
        return config_path

    for path in root_path.parents:
        config_path = next(path.glob(".jire.config"), None)

        if config_path:
            break

    if not config_path:
        raise ConfigError("Missing config file.")

    return config_path


def parse_config_path(path: pathlib.Path | None) -> dict[str, str]:
    """Parse the provided config file."""

    config = {}
    if not path:
        return config

    resolved_path = path.resolve().absolute()
    parser = configparser.ConfigParser()
    parser.read_string(resolved_path.read_text())

    try:
        config["project"] = parser["Project"]["name"]
        config["db_path"] = parser["Project"]["db_path"]
    except KeyError as ex:
        raise ConfigError(f"{ex}") from ex

    return config


def main() -> None:
    parser = argparse.ArgumentParser("Jirrrrrre!")
    parser.add_argument("--format", choices=("json", "console"), default="console")

    subparsers = parser.add_subparsers(required=True)
    ls_parser = subparsers.add_parser("ls")
    new_parser = subparsers.add_parser("new")
    todo_parser = subparsers.add_parser("todo")
    doing_parser = subparsers.add_parser("doing")
    done_parser = subparsers.add_parser("done")
    sync_parser = subparsers.add_parser("sync")

    ls_parser.add_argument("--search")
    ls_parser.add_argument("--assigned_to")
    ls_parser.add_argument("--created_by")
    ls_parser.add_argument("--status", choices=("TODO", "DOING", "DONE"))
    ls_parser.set_defaults(handler=list_ticket_handler)

    new_parser.add_argument("name")
    new_parser.add_argument("--description", default="")
    new_parser.add_argument("--assign_to")
    new_parser.add_argument("--project", default="")
    new_parser.set_defaults(handler=create_ticket_handler)

    todo_parser.add_argument("rowid")
    todo_parser.add_argument("--assign_to")
    todo_parser.set_defaults(handler=assign_todo_handler)

    doing_parser.add_argument("rowid")
    doing_parser.add_argument("--assign_to")
    doing_parser.set_defaults(handler=assign_doing_handler)

    done_parser.add_argument("rowid")
    done_parser.add_argument("--notes", default="")
    done_parser.set_defaults(handler=mark_as_done_handler)

    sync_parser.set_defaults(handler=sync_handler)

    # Call application
    cwd_path = pathlib.Path.cwd()

    try:
        config_path = find_config_path(root_path=cwd_path)
    except ConfigError:
        rich.print("[red]Missing config file: Please create .jire.config")
        exit(1)

    try:
        config = parse_config_path(path=config_path)
    except ConfigError as ex:
        rich.print(f"[red]Invalid config file: {ex}")
        exit(1)

    db_path = pathlib.Path(config["db_path"])

    if not db_path.is_absolute():
        db_path = config_path.parent / db_path

    conn = sqlite3.connect(db_path)
    setup_db(conn=conn)

    args = parser.parse_args()
    user = os.getlogin()
    current_datetime = datetime.datetime.now(tz=datetime.UTC)

    args.handler(
        args, conn=conn, user=user, current_datetime=current_datetime, config=config
    )


if __name__ == "__main__":
    main()
