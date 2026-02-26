Name: sharded-queue
Version: 1.1.0-1
Release: 1%{?dist}
Summary: Tarantool Sharded Queue Application
Group: Applications/Databases
License: BSD
URL: https://github.com/tarantool/sharded-queue
Source0: https://github.com/tarantool/%{name}/archive/%{version}/%{name}-%{version}.tar.gz
BuildArch: noarch
BuildRequires: tarantool >= 3.0.2.0
Requires: tarantool >= 3.0.2.0
%description
This module provides roles for the Tarantool 3 and for the Tarantool Cartridge
implementing of a distributed queue compatible with Tarantool queue (fifiottl driver).

%prep
%setup -q -n %{name}-%{version}

%install
install -d %{buildroot}%{_datarootdir}/tarantool/
install -m 0644 init.lua %{buildroot}%{_datarootdir}/tarantool/init.lua
install -d %{buildroot}%{_datarootdir}/tarantool/sharded_queue
install -m 0644 sharded_queue/* %{buildroot}%{_datarootdir}/tarantool/sharded_queue/
install -d %{buildroot}%{_datarootdir}/tarantool/roles
install -m 0644 roles/* %{buildroot}%{_datarootdir}/tarantool/roles/

%files
%{_datarootdir}/tarantool/sharded_queue
%{_datarootdir}/tarantool/init.lua
%{_datarootdir}/tarantool/roles
%doc README.md

%changelog

* Fri Feb 27 2026 Oleg Jukovec <oleg.jukovec@tarantool.org> - 1.1.0-1
- Method truncate() to clean tubes.
- `release_limit` configuration option: a taken task is removed from the queue
- `release_limit_policy` configuration option: control the behavior when a task

* Tue Feb 17 2026 Oleg Jukovec <oleg.jukovec@tarantool.org> 1.0.0-1
- Initial version of the RPM spec
