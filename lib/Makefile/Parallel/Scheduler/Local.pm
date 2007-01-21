package Makefile::Parallel::Scheduler::Local;

use base qw(Makefile::Parallel::Scheduler);

use strict;
use warnings;
use Proc::Simple;
use Data::Dumper;

sub new {
    my ($class, $self) = @_;

    $self ||= {};
    $self->{running} = 0;
    $self->{max}   ||= 1;

    bless $self, $class;
}

sub launch {
    my ($self, $job, $debug) = @_;

    # Launch the process
    my $proc = Proc::Simple->new();
    $proc->start($job->{action}->[0]->{shell});
    $job->{proc} = $proc;

    $self->{running}++;
}

sub poll {
    my ($self, $job, $logger) = @_;

    my $res = $job->{proc}->poll();

    $self->{running}-- unless $res;
    return $res;
}

sub interrupt {
    my ($self, $job) = @_;

    $self->{running}--;
    $job->{proc}->kill();
}

sub get_id {
    my ($self, $job) = @_;

    $job->{proc}->pid;
}

sub can_run {
    my ($self) = @_;

    $self->{running} != $self->{max};
}

sub clean {
    my ($self, $queue) = @_;

    return;
}

sub get_dead_job_info {
    my ($self, $job) = @_;

    $job->{exitstatus} = $job->{proc}->exit_status();
    # TODO: get realtime.. it's not too difficult
}

1;
