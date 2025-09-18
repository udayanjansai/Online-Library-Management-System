#!/usr/bin/env python3
"""
library_cli.py
A simple command-line Library Management using Supabase (Postgres) and supabase-py.

Requirements:
    pip install supabase
Set environment variables:
    SUPABASE_URL, SUPABASE_KEY
"""

import os
import sys
from supabase import create_client
from datetime import datetime, timedelta

SUPABASE_URL = os.getenv("SUPABASE_URL") or "https://YOUR_PROJECT_REF.supabase.co"
SUPABASE_KEY = os.getenv("SUPABASE_KEY") or "YOUR_SERVICE_ROLE_KEY"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def add_member(name: str, email: str):
    resp = supabase.table("members").insert({"name": name, "email": email}).execute()
    if resp.error:
        print("Error:", resp.error)
    else:
        print("Member added:", resp.data)


def add_book(title: str, author: str, category: str = None, stock: int = 1):
    row = {"title": title, "author": author, "category": category, "stock": stock}
    resp = supabase.table("books").insert(row).execute()
    if resp.error:
        print("Error:", resp.error)
    else:
        print("Book added:", resp.data)


def list_books():
    resp = supabase.table("books").select("*").order("book_id").execute()
    if resp.error:
        print("Error:", resp.error)
        return
    print("Books:")
    for r in resp.data:
        print(f"{r['book_id']:3} | {r['title'][:40]:40} | {r['author'][:20]:20} | stock: {r.get('stock',0)}")


def search_books(q: str):
    # simple ilike search on title/author/category (Postgres)
    pattern = f"%{q}%"
    resp = supabase.table("books").select("*").or_(f"title.ilike.{pattern},author.ilike.{pattern},category.ilike.{pattern}").execute()
    if resp.error:
        print("Error:", resp.error)
        return
    print("Search results:")
    for r in resp.data:
        print(f"{r['book_id']:3} | {r['title'][:40]:40} | {r['author'][:20]:20} | stock: {r.get('stock',0)}")


def show_member(member_id: int):
    mem = supabase.table("members").select("*").eq("member_id", member_id).single().execute()
    if mem.error:
        print("Error:", mem.error)
        return
    if not mem.data:
        print("Member not found")
        return
    print("Member:", mem.data)
    # list borrowed books (active and history)
    resp = supabase.table("borrow_records").select("record_id, book_id, borrow_date, return_date, books(title,author)").eq("member_id", member_id).order("borrow_date", desc=False).execute()
    if resp.error:
        print("Error fetching borrow records:", resp.error)
        return
    print("Borrow records:")
    for r in resp.data:
        title = r.get("books", {}).get("title") if r.get("books") else None
        status = "Returned" if r.get("return_date") else "Borrowed"
        print(f"  record {r['record_id']:3} | book {r['book_id']:3} | {title or 'N/A'} | {r['borrow_date']} -> {r['return_date']} | {status}")


def update_book_stock(book_id: int, new_stock: int):
    resp = supabase.table("books").update({"stock": new_stock}).eq("book_id", book_id).execute()
    if resp.error:
        print("Error:", resp.error)
    else:
        print("Updated:", resp.data)


def update_member_info(member_id: int, name: str = None, email: str = None):
    payload = {}
    if name:
        payload["name"] = name
    if email:
        payload["email"] = email
    if not payload:
        print("Nothing to update.")
        return
    resp = supabase.table("members").update(payload).eq("member_id", member_id).execute()
    if resp.error:
        print("Error:", resp.error)
    else:
        print("Updated member:", resp.data)


def delete_member(member_id: int):
    # allow deletion only if no active borrows (no borrow_records with return_date IS NULL)
    resp = supabase.table("borrow_records").select("*").eq("member_id", member_id).is_("return_date", "null").execute()
    if resp.error:
        print("Error checking borrow records:", resp.error)
        return
    if resp.data:
        print("Cannot delete: member has active borrowed books.")
        return
    resp2 = supabase.table("members").delete().eq("member_id", member_id).execute()
    if resp2.error:
        print("Error deleting member:", resp2.error)
    else:
        print("Member deleted:", resp2.data)


def delete_book(book_id: int):
    # allow deletion only if no borrow_records referencing it
    resp = supabase.table("borrow_records").select("*").eq("book_id", book_id).execute()
    if resp.error:
        print("Error checking borrow records:", resp.error)
        return
    if resp.data:
        print("Cannot delete: book has borrow history. Consider setting stock to 0 or archiving.")
        return
    resp2 = supabase.table("books").delete().eq("book_id", book_id).execute()
    if resp2.error:
        print("Error deleting book:", resp2.error)
    else:
        print("Book deleted:", resp2.data)


def borrow_book(member_id: int, book_id: int):
    # Call the RPC function we created: borrow_book(p_member_id int, p_book_id int)
    resp = supabase.rpc("borrow_book", {"p_member_id": member_id, "p_book_id": book_id}).execute()
    if resp.error:
        print("Error borrowing book:", resp.error)
    else:
        print("Borrow successful:", resp.data)


def return_book(record_id: int):
    resp = supabase.rpc("return_book", {"p_record_id": record_id}).execute()
    if resp.error:
        print("Error returning book:", resp.error)
    else:
        print("Return successful:", resp.data)


def report_overdue(days: int = 14):
    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
    # borrow_records with return_date IS NULL and borrow_date < cutoff
    resp = supabase.table("borrow_records").select("record_id, member_id, book_id, borrow_date, members(name), books(title)").is_("return_date", "null").lt("borrow_date", cutoff).execute()
    if resp.error:
        print("Error:", resp.error)
        return
    print(f"Overdue (borrowed before {cutoff}):")
    for r in resp.data:
        print(f"{r['record_id']:4} | member {r['member_id']} {r.get('members',{}).get('name')} | book {r['book_id']} {r.get('books',{}).get('title')} | borrowed {r['borrow_date']}")


def report_most_borrowed(limit: int = 10):
    # Use Postgres aggregation via RPC? Simple approach: query borrow_records and aggregate client-side by book
    resp = supabase.table("borrow_records").select("book_id").execute()
    if resp.error:
        print("Error:", resp.error)
        return
    counts = {}
    for r in resp.data:
        counts[r["book_id"]] = counts.get(r["book_id"], 0) + 1
    items = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:limit]
    print("Most borrowed books:")
    for book_id, cnt in items:
        b = supabase.table("books").select("title, author").eq("book_id", book_id).single().execute()
        title = b.data.get("title") if b.data else "Unknown"
        print(f"{book_id:3} | {title[:50]:50} | borrowed {cnt} times")


def print_help():
    print("""
Available commands:
  add_member <name> <email>
  add_book <title> <author> <category> <stock>
  list_books
  search_books <query>
  show_member <member_id>
  update_book_stock <book_id> <new_stock>
  update_member_info <member_id> [name=<name>] [email=<email>]
  delete_member <member_id>
  delete_book <book_id>
  borrow_book <member_id> <book_id>
  return_book <record_id>
  report_overdue [days]
  report_most_borrowed [limit]
  help
  exit
""")


def main_loop():
    print("Library CLI (Supabase). Type 'help' for commands.")
    while True:
        try:
            cmd = input("lib> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nExiting.")
            break
        if not cmd:
            continue
        parts = cmd.split()
        name = parts[0].lower()
        args = parts[1:]

        try:
            if name == "add_member":
                add_member(args[0], args[1])
            elif name == "add_book":
                # title and author may contain spaces; naive parsing:
                if len(args) < 4:
                    print("Usage: add_book <title> <author> <category> <stock>")
                else:
                    title, author, category, stock = args[0], args[1], args[2], int(args[3])
                    add_book(title, author, category, stock)
            elif name == "list_books":
                list_books()
            elif name == "search_books":
                search_books(" ".join(args))
            elif name == "show_member":
                show_member(int(args[0]))
            elif name == "update_book_stock":
                update_book_stock(int(args[0]), int(args[1]))
            elif name == "update_member_info":
                member_id = int(args[0])
                kv = {}
                for v in args[1:]:
                    if "=" in v:
                        k, val = v.split("=", 1)
                        kv[k] = val
                update_member_info(member_id, name=kv.get("name"), email=kv.get("email"))
            elif name == "delete_member":
                delete_member(int(args[0]))
            elif name == "delete_book":
                delete_book(int(args[0]))
            elif name == "borrow_book":
                borrow_book(int(args[0]), int(args[1]))
            elif name == "return_book":
                return_book(int(args[0]))
            elif name == "report_overdue":
                days = int(args[0]) if args else 14
                report_overdue(days)
            elif name == "report_most_borrowed":
                limit = int(args[0]) if args else 10
                report_most_borrowed(limit)
            elif name == "help":
                print_help()
            elif name == "exit":
                break
            else:
                print("Unknown command. Type 'help'.")
        except Exception as e:
            print("Error executing command:", e)


if __name__ == "__main__":
    main_loop()
